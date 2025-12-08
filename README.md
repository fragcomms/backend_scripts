Run python3 dump_header.py to view decoded header protobuf (ENSURE dump_header.py IS UPDATED WITH PATH TO DEMO FILE)

bitreader stuff is important for if we actually develop the entire thing ourselves. After the header, my understanding is the dem file has a non-protobuf format indication of the intended command, and then a protobuf payload.

Also worth noting a finished python parser is probably literally 30 times slower than the rust version