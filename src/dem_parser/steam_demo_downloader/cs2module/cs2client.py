from steam.client.gc import GameCoordinator
from . import cstrike15_gcmessages_pb2 as cstrike_protos
from . import gcsdk_gcmessages_pb2 as gcsdk_protos
import logging, gevent
from steam.core.msg import GCMsgHdrProto

class CS2Client(GameCoordinator):
    def __init__(self, steam):
        GameCoordinator.__init__(self, steam, 730) # 730 is csgo id, same as cs2
        self.target_match_code = None
        self.PROTO_MAP = {
            4004: gcsdk_protos.CMsgClientWelcome,
            9139: cstrike_protos.CMsgGCCStrike15_v2_MatchList
        }
        
    def set_target_match(self, sharecode):
        from csgo.sharecode import decode
        self.target_match_code = decode(sharecode)
        
    def request_match_info(self):
        if not self.target_match_code:
            logging.error("No target match set")
            return
        
        logging.info(f"Requesting match details for: {self.target_match_code['matchid']}")
        
        req = cstrike_protos.CMsgGCCStrike15_v2_MatchListRequestFullGameInfo()
        req.matchid = self.target_match_code['matchid']
        req.outcomeid = self.target_match_code['outcomeid']
        req.token = self.target_match_code['token']
        
        header = GCMsgHdrProto(9147)
        self.send(header, req.SerializeToString())

    def _process_gc_message(self, emsg, header, body):
        clean_id = emsg & 0x7FFFFFFF # valve ORs their id, im undoing it

        if clean_id in self.PROTO_MAP:
            try:
                # Instantiate the correct class
                proto_class = self.PROTO_MAP[clean_id]
                parsed_msg = proto_class()
                
                # Parse the raw bytes
                parsed_msg.ParseFromString(body)
                
                logging.debug(f"GC Message {clean_id} parsed successfully.")
                
                self.emit(clean_id, parsed_msg)
                
                return
                
            except Exception as e:
                logging.error(f"Failed to parse GC message {clean_id}: {e}")   
        return super()._process_gc_message(emsg, header, body)

    def send_hello(self):
        # Use the Proto from the module
        hello = gcsdk_protos.CMsgClientHello()
        hello.version = 2000682  # CS2 version (grabbed the most modern one 11/23/2025)
        
        logging.info("Sending 4006 to GC")
        header = GCMsgHdrProto(4006) # EMsgGCClientHello
        self.send(header, hello.SerializeToString()) # string conversion before send because steam wants it that way