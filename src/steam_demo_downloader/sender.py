import socket
import sys

def send_sharecode(code):
    try:
        # Connect to the bot's port
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(('127.0.0.1', 6000))
            
            # Send the code followed by a newline
            message = f"{code}\n"
            s.sendall(message.encode('utf-8'))
            
            print(f"Sent: {code}")
            
    except ConnectionRefusedError:
        print("Error: Bot is not running or not listening on port 6000.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 sender.py CSGO-XXXX-XXXX")
        sys.exit(1)
        
    send_sharecode(sys.argv[1])