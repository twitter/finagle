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
--

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

function dprint (s)
end
local dprint2 = function() end
dprint2 = dprint

dprint2("Wireshark version = ".. get_version())
dprint2("Lua version = ".. _VERSION)

----------------------------------------
-- Unfortunately, the older Wireshark/Tshark versions have bugs, and part of the point
-- of this script is to test those bugs are now fixed.  So we need to check the version
-- end error out if it's too old.
local major, minor, micro = get_version():match("(%d+)%.(%d+)%.(%d+)")
if major and tonumber(major) <= 1 and ((tonumber(minor) <= 10) or (tonumber(minor) == 11 and tonumber(micro) < 3)) then
        error(  "Sorry, but your Wireshark/Tshark version ("..get_version()..") is too old for this script!\n"..
                "This script needs Wireshark/Tshark version 1.11.3 or higher.\n" )
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

local MIN_TAG = TAG_PING + 1
local MAX_TAG = bit32.lshift(1, 23) - 1
local MSB_TAG = bit32.lshift(1, 23)

local MSG_TYPE_TREQ = 1
local MSG_TYPE_RREQ = 255 -- -1 & 0xff

local MSG_TYPE_TDISPATCH = 2
local MSG_TYPE_RDISPATCH = 254 -- -2 & 0xff

local MSG_TYPE_TDRAIN = 64
local MSG_TYPE_RDRAIN = 192 -- -64 & 0xff

local MSG_TYPE_TPING = 65
local MSG_TYPE_RPING = 191 -- -65 & 0xff

local MSG_TYPE_TDISCARDED = 66
local MSG_TYPE_RDISCARDED = 190 -- -66 & 0xff

local MSG_TYPE_TLEASE = 67

local MSG_TYPE_TINIT = 68
local MSG_TYPE_RINIT = 188 -- -68 & 0xff

local MSG_TYPE_RERR = 128 -- -128 & 0xff

local MESSAGE_TYPES = {
  [MSG_TYPE_TREQ] = "Treq",
  [MSG_TYPE_RREQ] = "Rreq",
  [MSG_TYPE_TDISPATCH] = "Tdispatch",
  [MSG_TYPE_RDISPATCH] = "Rdispatch",
  [MSG_TYPE_TDRAIN] = "Tdrain",
  [MSG_TYPE_RDRAIN] = "Rdrain",
  [MSG_TYPE_TPING] = "Tping",
  [MSG_TYPE_RPING] = "Rping",
  [MSG_TYPE_TDISCARDED] = "Tdiscarded",
  [MSG_TYPE_RDISCARDED] = "Rdiscarded",
  [MSG_TYPE_TLEASE] = "Tlease",
  [MSG_TYPE_TINIT] = "Tinit",
  [MSG_TYPE_RINIT] = "Rinit",
  [MSG_TYPE_RERR] = "Rerr"
}

----------------------------------------
-- creates a Proto object, but doesn't register it yet
local mux = Proto("mux","Mux Protocol")

----------------------------------------
-- multiple ways to do the same thing: create a protocol field (but not register it yet)
-- the abbreviation should always have "<myproto>." before the specific abbreviation, to avoid collisions
local pf_size = ProtoField.uint32("mux.size", "Size")
local pf_type = ProtoField.uint8("mux.type", "Message type", base.DEC, MESSAGE_TYPES)
local pf_tag = ProtoField.uint24("mux.tag", "Tag")
local pf_contexts = ProtoField.bytes("mux.context", "Contexts")
local pf_context_key = ProtoField.string("mux.context_key", "Context key")
local pf_dest = ProtoField.string("mux.dest", "Destination")
local pf_payload_len = ProtoField.string("mux.payload", "Payload Length")
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

----------------------------------------
-- The following creates the callback function for the dissector.
-- The 'tvbuf' is a Tvb object, 'pktinfo' is a Pinfo object, and 'root' is a TreeItem object.
-- Whenever Wireshark dissects a packet that our Proto is hooked into, it will call
-- this function and pass it these arguments for the packet it's dissecting.
function mux.dissector(tvbuf,pktinfo,root)
    dprint2("mux.dissector called")

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
    local size = tvbuf:range(0,4):int()

    -- now let's add the type, which are all in the packet bytes at offset 4 of length 1
    -- instead of calling this again and again, let's just use a variable
    tree:add(pf_type, tvbuf:range(4,1))

    local typeint = tvbuf:range(4,1):uint()
    local typestr = MESSAGE_TYPES[typeint]
    if not typestr then
      typestr = "unknown type (".. typeint .. ")"
    end

    -- now add more to the main tree
    tree:add(pf_tag, tvbuf:range(5,3))
    local tag = tvbuf:range(5,3):uint()

    pktinfo.cols.info:prepend(typestr .." Tag=".. tag .." ")

    local pos = 8
    if typeint == MSG_TYPE_RERR or typeint == MSG_TYPE_TDISCARDED then
      -- size remaining bytes are the message (-4 bytes for the type and tag)
      tree:add(pf_why, tvbuf(pos, size - 4))

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
    pos = pos + remaining

    dprint2("mux.dissector returning ".. pos)

    -- tell wireshark how much of tvbuf we dissected
    return pos
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
local function heur_dissect_mux(tvbuf,pktinfo,root)
    dprint2("heur_dissect_mux called")

    -- if our preferences tell us not to do this, return false
    if not default_settings.heur_enabled then
        dprint2("heur_dissect_mux disabled")
        return false
    end

    if tvbuf:len() < MIN_MUX_LEN then
        dprint("heur_dissect_mux: tvb shorter than MIN_MUX_LEN of: ".. MIN_MUX_LEN)
        return false
    end

    local tvbr = tvbuf:range(0,MIN_MUX_LEN)

    -- the first 4 bytes are size id, validate that it is the right size
    local size = tvbuf:range(0, 4):int()
    if size + 4 ~= tvbuf:len() then
      dprint("heur_dissect_mux: size did not match buf length: ".. size .." != ".. tvbuf:len())
      return false
    end

    -- the next byte is the type which we need to verify is valid
    local typeint = tvbuf:range(4,1):uint()
    local typestr = MESSAGE_TYPES[typeint]
    if not typestr then
      dprint("heur_dissect_mux: invalid type: ".. typeint)
      return false
    end

    -- examine the tag
    local tag = tvbuf:range(5,3):uint()
    if typeint == MSG_TYPE_TPING or typeint == MSG_TYPE_RPING then
      if tag ~= TAG_PING then
        dprint("heur_dissect_mux: invalid ping tag: ".. tag)
        return false
      end
    else
      if isFragment(tag) == true then
        tag = bit.band(tag, bit.bnot(MSB_TAG))
      end
      if tag < MIN_TAG or tag > MAX_TAG then
        dprint("heur_dissect_mux: invalid tag: ".. tag)
        return false
      end
    end

    dprint2("heur_dissect_mux: everything looks good calling the real dissector")


    -- ok, looks like it's ours, so go dissect it
    -- note: calling the dissector directly like this is new in 1.11.3
    -- also note that calling a Dissector object, as this does, means we don't
    -- get back the return value of the dissector function we created previously
    -- so it might be better to just call the function directly instead of doing
    -- this, but this script is used for testing and this tests the call() function
    mux.dissector(tvbuf,pktinfo,root)

    return true
end

----------------------------------------
-- we want to have our protocol dissection invoked for a specific tcp port,
-- so get the tcp dissector table and add our protocol to it
DissectorTable.get("tcp.port"):add(default_settings.port, mux)

-- now register that heuristic dissector into the tcp heuristic list
mux:register_heuristic("tcp",heur_dissect_mux)

-- We're done!
-- our protocol (Proto) gets automatically registered after this script finishes loading
----------------------------------------

