/* Read-only Darwin mapped-vnode observer. Build explicitly against the local SDK.
 * SDK types/API: libproc.h and sys/proc_info.h. See TD-005 for scope and sources.
 * Numeric argv: pid uid maximum_regions remaining_ms (device inode){1,33}.
 * This observes mappings; anonymous regions are not primary-image evidence.
 */
#include <libproc.h>
#include <sys/proc_info.h>
#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static int number(const char *text,uint64_t *value){
    if(!text[0]||(text[0]=='0'&&text[1]))return 0;
    for(const char *p=text;*p;p++)if(*p<'0'||*p>'9')return 0;
    char *end=NULL;errno=0;unsigned long long result=strtoull(text,&end,10);
    if(errno||!end||*end)return 0;
    *value=(uint64_t)result;return 1;
}
static uint64_t milliseconds(void){
    struct timespec value;
    if(clock_gettime(CLOCK_MONOTONIC,&value)!=0)return UINT64_MAX;
    return (uint64_t)value.tv_sec*1000+(uint64_t)value.tv_nsec/1000000;
}
int main(int argc,char **argv){
    if(argc<7||argc>71||(argc-5)%2)return 2;
    uint64_t values[70];
    for(int i=1;i<argc;i++)if(!number(argv[i],&values[i-1]))return 2;
    if(!values[0]||values[0]>INT_MAX||values[1]>UINT32_MAX||!values[2]||values[2]>4096||!values[3]||values[3]>5000)return 2;
    int pid=(int)values[0];uint32_t uid=(uint32_t)values[1];
    struct proc_bsdinfo before={0},after={0};
    uint64_t begun=milliseconds();
    if(begun==UINT64_MAX)return 2;
    if(proc_pidinfo(pid,PROC_PIDTBSDINFO,0,&before,sizeof before)!=(int)sizeof before||before.pbi_pid!=(uint32_t)pid||before.pbi_uid!=uid)return 2;
    uint64_t address=0,regions=0,files=0,missing=0,matches=0,executable=0;int terminal=0;
    while(regions<values[2]){
        uint64_t now=milliseconds();
        if(now==UINT64_MAX||now<begun||now-begun>=values[3])return 2;
        struct proc_regionwithpathinfo info={0};errno=0;
        int count=proc_pidinfo(pid,PROC_PIDREGIONPATHINFO,address,&info,sizeof info);
        if(count!=(int)sizeof info){
            if(count==0&&errno==EINVAL){terminal=EINVAL;break;}
            return 2;
        }
        regions++;
        uint64_t next=info.prp_prinfo.pri_address+info.prp_prinfo.pri_size;
        if(!info.prp_prinfo.pri_size||info.prp_prinfo.pri_address<address||next<=address||next<info.prp_prinfo.pri_address)return 2;
        address=next;
        struct vinfo_stat *v=&info.prp_vip.vip_vi.vi_stat;
        if(v->vst_dev&&v->vst_ino){
            files++;if(info.prp_prinfo.pri_protection&4)executable++;
            for(int i=4;i<argc-1;i+=2)if((uint64_t)v->vst_dev==values[i]&&v->vst_ino==values[i+1])matches++;
        }else missing++;
    }
    uint64_t ended=milliseconds();
    if(terminal!=EINVAL||!regions||!files||ended==UINT64_MAX||ended<begun||ended-begun>=values[3])return 2;
    if(proc_pidinfo(pid,PROC_PIDTBSDINFO,0,&after,sizeof after)!=(int)sizeof after)return 2;
    int stable=(before.pbi_pid==after.pbi_pid&&before.pbi_uid==after.pbi_uid&&before.pbi_start_tvsec==after.pbi_start_tvsec&&before.pbi_start_tvusec==after.pbi_start_tvusec);
    if(!stable||before.pbi_start_tvusec>=1000000)return 2;
    printf("{\"profile\":\"ashlar-darwin-mapped-vnode-observation/0.1\",\"pid\":%d,\"owner_uid\":%" PRIu32 ",\"start_seconds\":%" PRIu64 ",\"start_microseconds\":%" PRIu64 ",\"regions\":%" PRIu64 ",\"file_backed_regions\":%" PRIu64 ",\"executable_file_regions\":%" PRIu64 ",\"no_vnode_regions\":%" PRIu64 ",\"selected_vnode_matches\":%" PRIu64 ",\"terminal_errno\":%d,\"stable_bsd_identity\":true}\n",pid,uid,before.pbi_start_tvsec,before.pbi_start_tvusec,regions,files,executable,missing,matches,terminal);
    return 0;
}
