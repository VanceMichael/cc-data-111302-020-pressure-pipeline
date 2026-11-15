import http from 'node:http';
import {readFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import { gzipSync } from 'node:zlib';
import { once } from 'node:events';
export function json(response,value,status=200){response.writeHead(status,{'Content-Type':'application/json; charset=utf-8'});response.end(JSON.stringify(value));}
// 记录内容:默认高可压缩('x'重复);noise=true 时用确定性的伪随机十六进制,
// 压缩率接近随机数据,且重启后内容不变,恢复路径可以安全重放。
function recordValue(state,index,size){if(!state.noise)return 'x'.repeat(size);let out='';let seed=`seed-${index}`;while(out.length<size){out+=createHash('sha256').update(seed).digest('hex');seed=out.slice(-64);}return out.slice(0,size);}
export async function createSource({port=0,host='127.0.0.1'}={}){
  const state=JSON.parse(await readFile(new URL('../fixtures/source.json',import.meta.url),'utf8'));
  
  const pages={};
  const server=http.createServer(async(request,response)=>{
    try{
      const url=new URL(request.url,'http://local.test');let text='';
      for await(const chunk of request){text+=chunk;if(text.length>8*1024*1024){json(response,{error:'控制请求过大'},413);return;}}
      let body={};if(text){try{body=JSON.parse(text);}catch{return json(response,{error:'JSON无效'},400);}}
      if(url.pathname==='/health')return json(response,{status:'ok'});
      if(url.pathname==='/state'||url.pathname==='/inventory')return json(response,state);
      if(url.pathname==='/control'&&request.method==='POST'){Object.assign(state,body); return json(response,state);}
      if(Object.hasOwn(pages,url.pathname)){response.setHeader('Content-Type','text/html; charset=utf-8');return response.end(await readFile(new URL('../public/'+pages[url.pathname],import.meta.url)));}
      
    if(url.pathname==='/stream'){
      response.setHeader('Content-Type','application/x-ndjson');response.setHeader('Content-Encoding','gzip');
      const sizes=Array.isArray(state.recordSizes)&&state.recordSizes.length>0?state.recordSizes:Array.from({length:state.count},()=>state.recordBytes);
      const raw=sizes.map((size,index)=>JSON.stringify({id:`r${index}`,value:recordValue(state,index,Math.max(0,size))})+'\n').join('');const data=gzipSync(raw);
      for(let offset=0;offset<data.length;offset+=state.chunkBytes){if(response.destroyed)break;if(!response.write(data.subarray(offset,offset+state.chunkBytes)))await once(response,'drain');if(state.delay)await new Promise(resolve=>setTimeout(resolve,state.delay));}return response.end();
    }
    if(url.pathname==='/sink'&&request.method==='POST'){
      if(!body.batchId||!Array.isArray(body.records))return json(response,{error:'提交格式无效'},400);if(state.rejectBatch)return json(response,{error:'整批拒收'},422);
      const fingerprint=createHash('sha256').update(JSON.stringify(body.records)).digest('hex');const previous=state.accepted[body.batchId];if(previous&&previous.fingerprint!==fingerprint)return json(response,{error:'提交冲突'},409);
      const receipt=previous||{batchId:body.batchId,fingerprint,count:body.records.length};state.accepted[body.batchId]=receipt;if(state.sinkDelay)await new Promise(resolve=>setTimeout(resolve,state.sinkDelay));if(state.dropAck)return response.destroy();return json(response,receipt);
    }
    if(url.pathname.startsWith('/sink/')){const receipt=state.accepted[url.pathname.split('/').pop()];return json(response,receipt||{error:'未接受'},receipt?200:404);}

      return json(response,{error:'资源不存在'},404);
    }catch(error){if(!response.headersSent)json(response,{error:error.message},500);else response.destroy();}
  });
  await new Promise(resolve=>server.listen(port,host,resolve));
  return {url:`http://127.0.0.1:${server.address().port}`,state,close:async()=>{server.closeAllConnections();await new Promise(resolve=>server.close(resolve));}};
}
