----------------------------------------
--
-- A wireshark dissector for Mux.
--
-- See README.md for instructions.
--
-- Disclaimers:
-- This is based on Wireshark's example Lua dissector for DNS.
-- There are almost certainly horrible things afoot here
-- as I cobbled it together without knowing either Lua or Wireshark dissectors.
--
-- There is an example pcap file in finagle-mux/src/test/resources/end-to-end.pcap
-- created by capturing the output of finagle-thriftmux's EndToEndTest.


----------------------------------------
-- creates a Proto object, but doesn't register it yet
local mux = Proto("mux","Mux Protocol")

local debug_level = {
    DISABLED = 0,
    LEVEL_1  = 1,
    LEVEL_2  = 2
}


-- set this DEBUG to debug_level.LEVEL_1 to enable printing debug_level info
-- set it to debug_level.LEVEL_2 to enable really verbose printing
-- note: this will be overridden by user's preference settings
local DEBUG = debug_level.LEVEL_1

local default_settings =
{
    debug_level  = DEBUG,
    port         = 9990,
    heur_enabled = true,
}

local dprint = function() end
local dprint2 = function() end
local function resetDebugLevel()
    if default_settings.debug_level > debug_level.DISABLED then
        dprint = function(...)
            print(table.concat({"Lua: ", ...}," "))
        end

        if default_settings.debug_level > debug_level.LEVEL_1 then
            dprint2 = dprint
        end
    else
        dprint = function() end
        dprint2 = dprint
    end
end
resetDebugLevel()

dprint2("Wireshark version = ".. get_version())
dprint2("Lua version = ".. _VERSION)

----------------------------------------
-- the lua api for tcp segment reassembly was introduced in 1.99.2
local major, minor, micro = get_version():match("(%d+)%.(%d+)%.(%d+)")
if major and tonumber(major) <= 1 and ((tonumber(minor) <= 98) or (tonumber(minor) == 99 and tonumber(micro) < 2)) then
  error(  "Sorry, but your Wireshark/Tshark version ("..get_version()..") is too old for this script!\n"..
          "This script needs Wireshark/Tshark version 1.99.2 or higher.\n" )
end

-- more sanity checking
-- verify we have the ProtoExpert class in wireshark, as that's the newest thing this file uses
assert(ProtoExpert.new, "Wireshark does not have the ProtoExpert class, so it's too old - get the latest 1.11.3 or higher")

----------------------------------------

----------------------------------------
---- some constants for later use ----
-- the min header size
-- 4 bytes for size, 1 for type, 3 for tag number
local MIN_MUX_LEN = 8
local TAG_MARKER = 0
local TAG_PING = 1


-- magic value defined in finagle/finagle-mux/src/main/scala/com/twitter/finagle/mux/Handshake.scala
local CAN_TINIT_PAYLOAD = "tinit check"

local MIN_TAG = TAG_PING
local MAX_TAG = bit32.lshift(1, 23) - 1
local MSB_TAG = bit32.lshift(1, 23)

local MSG_TYPE_CAN_TINIT = 0
local MSG_TYPE_TREQ = 1
local MSG_TYPE_RREQ = 255 -- -1 & 0xff

local MSG_TYPE_TDISPATCH = 2
local MSG_TYPE_RDISPATCH = 254 -- -2 & 0xff

local MSG_TYPE_TDRAIN = 64
local MSG_TYPE_RDRAIN = 192 -- -64 & 0xff

local MSG_TYPE_TPING = 65
local MSG_TYPE_RPING = 191 -- -65 & 0xff

local MSG_TYPE_TDISCARDED = 66
local MSG_TYPE_OLD_TDISCARDED = 194 -- -62 & 0xff
local MSG_TYPE_RDISCARDED = 190 -- -66 & 0xff

local MSG_TYPE_TLEASE = 67

local MSG_TYPE_TINIT = 68
local MSG_TYPE_RINIT = 188 -- -68 & 0xff

local MSG_TYPE_RERR = 128 -- -128 & 0xff

local MSG_TYPE_OLD_RERR = 127

local MESSAGE_TYPES = {
  [MSG_TYPE_CAN_TINIT] = "CanTInit",
  [MSG_TYPE_TREQ] = "Treq",
  [MSG_TYPE_RREQ] = "Rreq",
  [MSG_TYPE_TDISPATCH] = "Tdispatch",
  [MSG_TYPE_RDISPATCH] = "Rdispatch",
  [MSG_TYPE_TDRAIN] = "Tdrain",
  [MSG_TYPE_RDRAIN] = "Rdrain",
  [MSG_TYPE_TPING] = "Tping",
  [MSG_TYPE_RPING] = "Rping",
  [MSG_TYPE_TDISCARDED] = "Tdiscarded",
  [MSG_TYPE_OLD_TDISCARDED] = "Tdiscarded",
  [MSG_TYPE_RDISCARDED] = "Rdiscarded",
  [MSG_TYPE_TLEASE] = "Tlease",
  [MSG_TYPE_TINIT] = "Tinit",
  [MSG_TYPE_RINIT] = "Rinit",
  [MSG_TYPE_RERR] = "Rerr",
  [MSG_TYPE_OLD_RERR] = "Rerr"
}

local MARKER_TYPES = {
  [MSG_TYPE_TDISCARDED] = true,
  [MSG_TYPE_OLD_TDISCARDED] = true,
  [MSG_TYPE_TLEASE] = true
}


----------------------------------------
-- multiple ways to do the same thing: create a protocol field (but not register it yet)
-- the abbreviation should always have "<myproto>." before the specific abbreviation, to avoid collisions
local pf_size = ProtoField.uint32("mux.size", "Size")
local pf_type = ProtoField.uint8("mux.type", "Message type", base.DEC, MESSAGE_TYPES)
local pf_tag = ProtoField.uint24("mux.tag", "Tag")
local pf_discard_tag = ProtoField.uint24("mux.discard_tag", "Tag To Discard")
local pf_contexts = ProtoField.bytes("mux.context", "Contexts")
local pf_context_key = ProtoField.string("mux.context_key", "Context key")
local pf_dest = ProtoField.string("mux.dest", "Destination")
local pf_payload_len = ProtoField.uint32("mux.payload", "Payload Length")
local pf_mux_overhead = ProtoField.uint32("mux.overhead", "Mux Overhead") -- the size of framing + contexts + dtabs + size field
local pf_why = ProtoField.string("mux.msg", "Why")
local pf_trace_flags = ProtoField.uint64("mux.ctx.trace.flags", "Flags", base.HEX)

mux.fields = {
  pf_size,
  pf_type,
  pf_tag,
  pf_contexts,
  pf_context_key,
  pf_dest,
  pf_payload_len,
  pf_mux_overhead,
  pf_discard_tag,
  pf_why,
  pf_trace_flags
}

-- Tag bit manipulation
--
function isFragment(tag)
  -- Corresponds to `Tags.isFragment`
  -- ((tag >> 23) & 1) == 1
  return bit.band(bit.rshift(tag, 23), 1) == 1
end

-- Tries to decode the context value for the given key.
-- Understands some of the primary contexts used in Finagle.
--
function decodeCtx(ctx_tree, key, value)
  local t = ctx_tree:add(key)
  local tvb = ByteArray.tvb(value)

  if key == "com.twitter.finagle.Deadline" then
    -- format is (big endian):
    --   8 bytes: timestamp in nanos since epoch
    --   8 bytes: deadline in nanos since epoch
    local timestamp = tvb:range(0, 8):uint64() / 1000
    local deadline = tvb:range(8, 8):uint64() / 1000
    t:add("Timestamp (micros after epoch): ".. timestamp)
    t:add("Deadline (micros after epoch): ".. deadline)

  elseif key == "com.twitter.finagle.tracing.TraceContext" then
    -- format is (big endian):
    --   8 bytes: span id
    --   8 bytes: parent id
    --   8 bytes: trace id
    --   8 bytes: flags
    t:add("Span id: ".. tvb:range(0, 8):uint64())
    t:add("Parent id: ".. tvb:range(8, 8):uint64())
    t:add("Trace id: ".. tvb:range(16, 8):uint64())
    t:add(pf_trace_flags, tvb:range(24, 8))

  elseif key == "com.twitter.finagle.thrift.ClientIdContext" then
    -- a utf8 string
    t:add("Name: ".. tvb:range():string())

  else
    t:add("length=".. value:len())
  end
end

function mux.dissector(tvbuf, pktinfo, root)
  -- heur_dissect_mux says it looks like ours, 
  -- call tcp desegmenter with our full dissector
  dprint("mux.dissector CALLED")
  return dissect_tcp_pdus(tvbuf, root, MIN_MUX_LEN, heur_get_mux_length, mux_dissector_impl, true)
end

function looks_like_mux(tvbuf,pktinfo,root)
  dprint2("looks_like_mux called")

  -- if our preferences tell us not to do this, return false
  if not default_settings.heur_enabled then
    dprint("looks_like_mux: heur_dissect_mux disabled")
    return false
  end

  if tvbuf:len() < MIN_MUX_LEN then
    dprint("looks_like_mux: tvb shorter than MIN_MUX_LEN of: ".. MIN_MUX_LEN)
    return false
  end

  -- the first 4 bytes are the size of the mux message
  local size = tvbuf:range(0, 4):uint()

  -- the next byte is the type which we need to verify is valid
  local typeint = tvbuf:range(4,1):uint()
  local typestr = MESSAGE_TYPES[typeint]
  if not typestr then
    dprint("looks_like_mux: invalid type: ".. typeint)
    return false
  end

  -- examine the tag
  local tag = tvbuf:range(5,3):uint()
  if typeint == MSG_TYPE_TPING or typeint == MSG_TYPE_RPING then
    if tag ~= TAG_PING then
      dprint("looks_like_mux: invalid ping tag: ".. tag)
      return false
    end
  else
    if MARKER_TYPES[typeint] then
      if tag ~= TAG_MARKER then
        dprint(string.format("%s (%d) %s (%d)", "heur_dissect_mux: marker type", typeint, "with non-marker tag", tag))
        return false
      end
    else
      if isFragment(tag) == true then
        tag = bit.band(tag, bit.bnot(MSB_TAG))
      end
      if tag < MIN_TAG or tag > MAX_TAG then
        dprint("looks_like_mux: invalid tag: ".. tag)
        return false
      end
    end
  end

  return true
end

local function heur_dissect_mux(tvbuf,pktinfo,root)
  if looks_like_mux(tvbuf, pktinfo, root) then
    dissect_tcp_pdus(tvbuf, root, MIN_MUX_LEN, heur_get_mux_length, mux_dissector_impl, true)
    return true
  else
    return false
  end
end

----------------------------------------
-- The following creates the callback function for the dissector.
-- The 'tvbuf' is a Tvb object, 'pktinfo' is a Pinfo object, and 'root' is a TreeItem object.
-- Whenever Wireshark dissects a packet that our Proto is hooked into, it will call
-- this function and pass it these arguments for the packet it's dissecting.
function mux_dissector_impl(tvbuf, pktinfo, root)
    dprint("mux_dissector_impl called")
    if looks_like_mux(tvbuf, pktinfo,root) == false then
      dprint("looks_like_mux returned false, bailing out")
      return 0
    end


    -- set the protocol column to show our protocol name
    pktinfo.cols.protocol:set("MUX")

    -- We want to check that the packet size is rational during dissection, so let's get the length of the
    -- packet buffer (Tvb).
    -- we can use tvb:len() or tvb:reported_len() here; but I prefer tvb:reported_length_remaining() as it's safer.
    local pktlen = tvbuf:reported_length_remaining()

    -- We start by adding our protocol to the dissection display tree.
    -- A call to tree:add() returns the child created, so we can add more "under" it using that return value.
    -- The second argument is how much of the buffer/packet this added tree item covers/represents - in this
    -- case that's the remainder of the packet.
    local tree = root:add(mux, tvbuf:range(0,pktlen))

    -- size starts at offset 0, for 4 bytes length.
    tree:add(pf_size, tvbuf:range(0,4))

    -- size represents the number of bytes remaining in the frame.
    local size = tvbuf:range(0,4):uint()

    -- now let's add the type, which are all in the packet bytes at offset 4 of length 1
    -- instead of calling this again and again, let's just use a variable

    local typeint = tvbuf:range(4,1):uint()
    local typestr = MESSAGE_TYPES[typeint]

    -- special case the magic RErr message used in handshaking
    if pktlen == 19 and typeint == MSG_TYPE_OLD_RERR and size == 15 and tvbuf:range(8, 11):string() == CAN_TINIT_PAYLOAD then -- magic!
      tree:add(pf_type, MSG_TYPE_CAN_TINIT)
      local typestr = "CanTinit"
    else
      tree:add(pf_type, tvbuf:range(4,1))
    end

    if not typestr then
      typestr = "unknown type (".. typeint .. ")"
    end

    -- now add more to the main tree
    tree:add(pf_tag, tvbuf:range(5,3))
    local tag = tvbuf:range(5,3):uint()

    pktinfo.cols.info:prepend(typestr .." Tag=".. tag .." ")

    local pos = 8

    if typeint == MSG_TYPE_RERR or typeint == MSG_TYPE_OLD_RERR then
      -- size remaining bytes are the message (-4 bytes for the type and tag)
      tree:add(pf_why, tvbuf(pos, size - 4))
    elseif typeint == MSG_TYPE_OLD_TDISCARDED or typeint == MSG_TYPE_TDISCARDED then
      tree:add(pf_discard_tag, tvbuf:range(pos, 3)) -- first three bytes after
                                                    -- tag are the 'discard_tag'
      tree:add(pf_why, tvbuf(pos + 3, size - 7)) -- remainder of body is 'why'

    elseif typeint == MSG_TYPE_TDISPATCH then
      -- 2 bytes for number of contexts
      -- for each context:
      --   2 bytes for context key's length, followed by len bytes for the key
      --   2 bytes for context values's length, followed by len bytes for the value
      -- 2 bytes for the dest's length, followed by len bytes of the dest
      -- 2 bytes for the number of dtab entries
      -- for each dtab entry:
      --   2 bytes for the entry's prefix bytes len, followed by len bytes of the src
      --   2 bytes for the entry's tree bytes len, followed by len bytes of the tree
      -- the remaining bytes are the payload.

      -- begin with contexts
      local numctxs = tvbuf:range(pos,2):uint()
      pos = pos + 2
      local ctx_tree = tree:add("Contexts: ".. numctxs)
      for ctx = 1,numctxs do
        local keylen = tvbuf:range(pos,2):uint()
        pos = pos + 2
        local key = tvbuf:range(pos, keylen):string()
        pos = pos + keylen
        local vallen = tvbuf:range(pos,2):uint()
        pos = pos + 2
        local value = tvbuf:range(pos,vallen):bytes()
        pos = pos + vallen

        decodeCtx(ctx_tree, key, value)
      end

      -- then dest
      local destlen = tvbuf:range(pos,2):uint()
      pos = pos + 2
      local dest = tvbuf:range(pos, destlen):string()
      if dest:len() > 0 then
        tree:add(pf_dest, dest)
      end
      pos = pos + destlen

      -- then dtabs
      local numdtabs = tvbuf:range(pos,2):uint()
      pos = pos + 2
      if numdtabs > 0 then
        local dtab_tree = tree:add("Dtabs")
        for entry = 1,numdtabs do
          local len = tvbuf:range(pos,2):uint()
          pos = pos + 2
          local src = tvbuf:range(pos, len):string()
          pos = pos + len
          len = tvbuf:range(pos,2):uint()
          pos = pos + 2
          local dst = tvbuf:range(pos, len):string()
          pos = pos + len

          dtab_tree:add(src .." => ".. dst)
        end
      end
    end

    local remaining = size - pos + 4 -- add back the 4 bytes for the size field
    tree:add(pf_payload_len, remaining)
    tree:add(pf_mux_overhead, size - remaining + 4) -- the size of framing + contexts + dtabs + size field
    pos = pos + remaining


    -- tell wireshark how much of tvbuf we dissected
    return pos
end

----------------------------------------
-- the following function is used for the new dissect_tcp_pdus method
-- this returns the length of the full message
function heur_get_mux_length(tvbuf, pktinfo, offset)
    return tvbuf:range(offset,4):uint() + 4
end


----------------------------------------
-- we also want to add the heuristic dissector, for any tcp protocol
-- first we need a heuristic dissection function
-- this is that function - when wireshark invokes this, it will pass in the same
-- things it passes in to the "dissector" function, but we only want to actually
-- dissect it if it's for us, and we need to return true if it's for us, or else false
-- figuring out if it's for us or not is not easy
-- we need to try as hard as possible, or else we'll think it's for us when it's
-- not and block other heuristic dissectors from getting their chance
--

----------------------------------------
-- we want to have our protocol dissection invoked for a specific tcp port,
-- so get the tcp dissector table and add our protocol to it
DissectorTable.get("tcp.port"):add(default_settings.port, mux)

-- register the heuristic dissector.
mux:register_heuristic("tcp", heur_dissect_mux)
