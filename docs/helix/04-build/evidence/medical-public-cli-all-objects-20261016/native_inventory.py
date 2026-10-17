import hashlib,stat
def native_files(selected):
 root=selected.root;targets={t.path for t in selected.targets};entries=[];total=0
 if len(targets)!=6 or any(t.parent!=root for t in targets):raise PermissionError('Exact six original native target directories required')
 for entry in sorted(root.iterdir()):
  if entry.is_symlink():raise PermissionError('Original native closure symlink refused')
  paths=sorted(entry.rglob('*'))if entry in targets else[entry]
  if entry not in targets and entry.is_dir():raise PermissionError('Unexpected original native control directory')
  for path in paths:
   st=path.lstat()
   if stat.S_ISDIR(st.st_mode):continue
   if not stat.S_ISREG(st.st_mode)or path.is_symlink():raise PermissionError('Original native closure requires regular files')
   if len(entries)>=4096 or st.st_size>134217728-total:raise ValueError('Native closure count/byte capacity exceeded')
   digest=hashlib.sha256();size=0
   with path.open('rb')as stream:
    while True:
     part=stream.read(min(65536,134217729-total-size))
     if not part:break
     size+=len(part)
     if size>st.st_size or total+size>134217728:raise PermissionError('Native closure changed or exceeded bytebound')
     digest.update(part)
   end=path.lstat()
   if size!=st.st_size or(st.st_dev,st.st_ino,st.st_size,st.st_mtime_ns,st.st_ctime_ns)!=(end.st_dev,end.st_ino,end.st_size,end.st_mtime_ns,end.st_ctime_ns):raise PermissionError('Native closure file changed during hashing')
   total+=size;entries.append({'path':str(path.relative_to(root)),'bytes':size,'sha256':digest.hexdigest()})
 return {'profile':'ashlar-original-medical-native-byte-closure/0.1','root':str(root),'maximum_files':4096,'maximum_bytes':134217728,'files':entries,'total_bytes':total,'scope':'Six selected native Delta directories and immediate original journal/control files only; content/inventory correspondence, no source or OS authority. SQLite SHM coordination compared separately.'}