namespace java com.twitter.delivery.thriftjava
#@namespace scala com.twitter.delivery.thriftscala

exception AException {
  1: i32 errorCode
}

service DeliveryService {
  Box getBox(1: AddrInfo addrInfo, 3: i8 passcode) throws (
    1: AException ex
  )

  list<Box> getBoxes(1: list<AddrInfo> listAddrInfo, 3: i8 passcode) throws (
    1: AException ex
  )

  string sendBox(1: Box box)

  string sendBoxes(1: list<Box> boxes)
}

struct Box {
  1: AddrInfo addrInfo;
  2: string item;
}

struct AddrInfo {
  1: string name;
  2: i32 zipCode;
}
