```bat
python runMessageClient.py 192.168.1.81 12005 getfile -k "testfile" -a "..\Batch\b.txt" -t -g 60
python runMessageClient.py 192.168.1.81 12005 getmessage -k "test" -t -g 60
python runMessageClient.py 192.168.1.81 12005 sendfile -k "testfile" -a "..\Batch\a.txt" -t 
python runMessageClient.py 192.168.1.81 12005 sendmessage -k "test" -a "success" 
python runMessageClient.py 192.168.1.81 12005 status

```