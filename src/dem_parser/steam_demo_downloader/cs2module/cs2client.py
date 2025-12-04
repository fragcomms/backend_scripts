from steam.client.gc import GameCoordinator
from cs2module.protobufs import (
  parse_gc_payload, gcmessages,
  k_EMsgGCClientWelcome, 
  k_EMsgGCCStrike15_v2_MatchListRequestFullGameInfo,
  k_EMsgGCCStrike15_v2_MatchList
)
import logging, gevent
from steam.core.msg import GCMsgHdrProto

class CS2Client(GameCoordinator):
    def __init__(self, steam):
        GameCoordinator.__init__(self, steam, 730) # 730 is csgo id, same as cs2
        self.target_match_code = None
        
    def set_target_match(self, match_dict):
        self.target_match_code = match_dict
        
    def request_match_info(self):
        if not self.target_match_code:
            logging.error("No target match set")
            return
        
        logging.info(f"Requesting match details for: {self.target_match_code['matchid']}")
        
        req = gcmessages.CMsgGCCStrike15_v2_MatchListRequestFullGameInfo()
        req.matchid = self.target_match_code['matchid']
        req.outcomeid = self.target_match_code['outcomeid']
        req.token = self.target_match_code['token']
        
        header = GCMsgHdrProto(k_EMsgGCCStrike15_v2_MatchListRequestFullGameInfo)
        self.send(header, req.SerializeToString())

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
            elif clean_id == 9139:
                logging.info(f"Match list found ({k_EMsgGCCStrike15_v2_MatchList})")
                for match in parsed_msg.matches:
                    logging.info(f"Match ID: {match.matchid}")
                    if len(match.roundstatsall) > 0:
                        last_round = match.roundstatsall[-1]
                        logging.info(f"Map Data: {last_round.map}")
                self.steam.disconnect()
        return super()._process_gc_message(emsg, header, body)
                

    def send_hello(self):
        # Use the Proto from the module
        hello = gcmessages.CMsgClientHello()
        hello.version = 2000682  # CS2 version (grabbed the most modern one 11/23/2025)
        
        logging.info("Sending 4006 to GC")
        header = GCMsgHdrProto(4006) # EMsgGCClientHello
        self.send(header, hello.SerializeToString()) # string conversion before send because steam wants it that way