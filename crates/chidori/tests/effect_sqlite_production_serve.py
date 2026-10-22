import json, os, pathlib, socket, sqlite3, subprocess, tempfile, threading, time, urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BIN = os.environ["CHIDORI_BIN"]

def port():
    s=socket.socket(); s.bind(("127.0.0.1",0)); p=s.getsockname()[1]; s.close(); return p
class Receiver(BaseHTTPRequestHandler):
    calls=0
    def do_POST(self): self.__class__.calls+=1; self.send_response(200); self.end_headers()
    def log_message(self,*args): pass
def request(p,path,body=None,timeout=10):
    data=None if body is None else json.dumps(body).encode()
    r=urllib.request.Request(f"http://127.0.0.1:{p}{path}",data=data,method="POST" if data else "GET",headers={"Content-Type":"application/json"} if data else {})
    try:
        with urllib.request.urlopen(r,timeout=timeout) as x:return x.status,x.read().decode()
    except urllib.error.HTTPError as x:return x.code,x.read().decode()
def case(trigger):
    with tempfile.TemporaryDirectory() as root:
        root=pathlib.Path(root); db=root/'runs.sqlite3'; rp=port(); Receiver.calls=0
        receiver=ThreadingHTTPServer(('127.0.0.1',rp),Receiver); threading.Thread(target=receiver.serve_forever,daemon=True).start()
        (root/'agent.ts').write_text("import {run} from 'chidori:agent';run(async()=>{await fetch('http://127.0.0.1:%s/effect',{method:'POST'});return {ok:true};});"%rp)
        sp=port(); env=os.environ.copy(); env.update({'CHIDORI_RUN_STORE':'sqlite','CHIDORI_RUN_DB':str(db),'CHIDORI_DURABILITY':'effect','CHIDORI_HTTP_ALLOW_HOSTS':'127.0.0.1','CHIDORI_ISOLATE':'off','CHIDORI_POLICY_PROFILE':'trusted'})
        proc=subprocess.Popen([BIN,'serve','agent.ts','--port',str(sp),'--no-isolate','--trusted'],cwd=root,env=env,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,text=True)
        try:
            for _ in range(200):
                try:
                    if request(sp,'/health')[0]==200: break
                except Exception: pass
                time.sleep(.05)
            else: raise AssertionError(proc.stderr.read())
            if trigger:
                sqlite3.connect(db).execute("CREATE TRIGGER reject_intent BEFORE INSERT ON run_blobs WHEN NEW.key LIKE 'host_promises/%' BEGIN SELECT RAISE(ABORT,'injected sqlite intent failure'); END;").connection.commit()
            status,body=request(sp,'/sessions',{'input':{},'session_id':'trigger' if trigger else 'control'})
            time.sleep(.4)
            records=sqlite3.connect(db).execute("SELECT data FROM run_records ORDER BY pos,seq").fetchall()
            http_records=[json.loads(record[0]) for record in records if json.loads(record[0]).get('function')=='http']
            response=json.loads(body)
            if trigger:
                assert status >= 400 or response.get('status')=='failed', (status,response)
                assert response.get('output') is None and response.get('status')!='completed', (status,response)
                assert 'injected sqlite intent failure' in response.get('error',''), response
                assert Receiver.calls==0 and not http_records, (status,response,Receiver.calls,records)
            else:
                assert Receiver.calls==1 and response.get('status')=='completed', (status,response,Receiver.calls)
                assert response.get('output') is not None and len(http_records)==1, (response,records)
                assert http_records[0].get('error') is None, http_records
        finally: proc.terminate(); proc.wait(timeout=5); receiver.shutdown()
case(False)
case(True)
