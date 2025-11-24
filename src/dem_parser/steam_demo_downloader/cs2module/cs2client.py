from steam.client.gc import GameCoordinator
from cs2module.protobufs import parse_gc_payload, gcmessages
import logging
from steam.core.msg import GCMsgHdrProto

class CS2Client(GameCoordinator):
    def __init__(self, steam):
        GameCoordinator.__init__(self, steam, 730) # 730 is csgo id, same as cs2

    def _process_gc_message(self, emsg, header, body):
        clean_id = emsg & 0x7FFFFFFF # valve ORs their id, im undoing it
        
        # body is already good to go
        parsed_msg = parse_gc_payload(clean_id, body)

        if parsed_msg:
            logging.debug(f"GC Message {clean_id} received. Payload: {parsed_msg}")
            
            # we want specific states, so 4004 means that the gc is up and running
            # only other important one is 9139, 9140, 9141, or 9147 for demo grabbing
            if clean_id == 4004:
                logging.info("GC welcomed")

    def send_hello(self):
        # Use the Proto from the module
        hello = gcmessages.CMsgClientHello()
        hello.version = 2000682  # CS2 version (grabbed the most modern one 11/23/2025)
        
        header = GCMsgHdrProto(4006) # EMsgGCClientHello
        self.send(header, hello.SerializeToString()) # string conversion before send because steam wants it that way