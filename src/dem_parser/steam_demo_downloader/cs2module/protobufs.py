import logging
from google.protobuf.descriptor import FieldDescriptor, Descriptor
from google.protobuf.message import Message
from google.protobuf.reflection import GeneratedProtocolMessageType

# --- ID Constants ---
k_EMsgGCClientWelcome = 4004
k_EMsgGCClientHello = 4006
k_EMsgGCClientConnectionStatus = 4009
k_EMsgGCCStrike15_v2_MatchListRequestFullGameInfo = 9126
k_EMsgGCCStrike15_v2_MatchList = 9127
k_EMsgGCCStrike15_v2_MatchmakingGC2ClientHello = 9173

# --- Type Constants (Standard Protobuf) ---
TYPE_UINT64 = 4
TYPE_INT32 = 5
TYPE_BYTES = 12
TYPE_UINT32 = 13

# --- CppType Constants (Must match google.protobuf.descriptor) ---
CPPTYPE_INT32 = 1
CPPTYPE_UINT32 = 3
CPPTYPE_UINT64 = 4
CPPTYPE_STRING = 9 # Used for both STRING and BYTES

# Mapping Field Types to Cpp Types
_TYPE_TO_CPP_TYPE = {
    TYPE_INT32: CPPTYPE_INT32,
    TYPE_UINT32: CPPTYPE_UINT32,
    TYPE_UINT64: CPPTYPE_UINT64,
    TYPE_BYTES: CPPTYPE_STRING,
}

def _create_proto_class(name, fields_dict):
    """Helper to dynamically create a Protobuf class."""
    fields = []
    for field_name, (index, field_type, label) in fields_dict.items():
        # Look up the correct CppType based on the FieldType
        cpp_type_val = _TYPE_TO_CPP_TYPE.get(field_type, 0)
        
        fd = FieldDescriptor(
            name=field_name,
            full_name=f'{name}.{field_name}',
            index=index - 1,
            number=index,
            type=field_type,
            cpp_type=cpp_type_val,
            label=label,
            has_default_value=False,
            default_value=None,
            message_type=None, 
            enum_type=None, 
            containing_type=None,
            is_extension=False, 
            extension_scope=None,
            options=None
        )
        fields.append(fd)

    desc = Descriptor(
        name=name,
        full_name=name,
        filename=None,
        containing_type=None,
        fields=fields,
        nested_types=[],
        enum_types=[],
        extensions=[],
        options=None,
        is_extendable=False,
        syntax='proto2'
    )
    
    return GeneratedProtocolMessageType(name, (Message,), {'DESCRIPTOR': desc})

class gcmessages:
    """Container for CS2 GC Protobuf definitions."""
    
    # CMsgClientHello: field 1 = version (uint32)
    CMsgClientHello = _create_proto_class('CMsgClientHello', {
        'version': (1, TYPE_UINT32, 1),             
        'client_session_need': (3, TYPE_UINT32, 1), 
        'client_launcher': (4, TYPE_UINT32, 1)      
    })

    # CMsgClientWelcome: field 1=version, 2=game_data (bytes), 4=game_data2 (bytes)
    CMsgClientWelcome = _create_proto_class('CMsgClientWelcome', {
        'version': (1, TYPE_UINT32, 1),
        'game_data': (2, TYPE_BYTES, 1),
        'game_data2': (4, TYPE_BYTES, 1)
    })

    # CMsgGCClientConnectionStatus: field 1=status (int32/enum)
    CMsgGCClientConnectionStatus = _create_proto_class('CMsgGCClientConnectionStatus', {
        'status': (1, TYPE_INT32, 1) 
    })

    # CMsgGCCStrike15_v2_MatchListRequestFullGameInfo
    # Used for requesting demos via sharecode
    CMsgGCCStrike15_v2_MatchListRequestFullGameInfo = _create_proto_class('CMsgGCCStrike15_v2_MatchListRequestFullGameInfo', {
        'matchid': (1, TYPE_UINT64, 1),   
        'outcomeid': (2, TYPE_UINT64, 1), 
        'token': (3, TYPE_UINT32, 1)     
    })
    
    # nothing is required, just an acknowledgement that matchmaking is up and running
    CMsgGCCStrike15_v2_MatchmakingGC2ClientHello = _create_proto_class('CMsgGCCStrike15_v2_MatchmakingGC2ClientHello', {})

# Mapping of EMsg IDs to the Protobuf Class
_PROTO_MAP = {
    k_EMsgGCClientHello: gcmessages.CMsgClientHello,
    k_EMsgGCClientWelcome: gcmessages.CMsgClientWelcome,
    k_EMsgGCClientConnectionStatus: gcmessages.CMsgGCClientConnectionStatus,
    k_EMsgGCCStrike15_v2_MatchListRequestFullGameInfo: gcmessages.CMsgGCCStrike15_v2_MatchListRequestFullGameInfo,
    k_EMsgGCCStrike15_v2_MatchmakingGC2ClientHello: gcmessages.CMsgGCCStrike15_v2_MatchmakingGC2ClientHello,
}

def parse_gc_payload(emsg, payload):
    """Parses a GC payload based on the EMsg ID."""
    proto_class = _PROTO_MAP.get(emsg) # emsg should be unmasked
    if proto_class:
        msg = proto_class()
        try:
            msg.ParseFromString(payload)
            return msg
        except Exception as e:
            logging.info(f"Error parsing proto for {emsg}: {e}")
            return None
    return None