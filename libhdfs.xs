#define PERL_NO_GET_CONTEXT
#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include "ppport.h"

#include <errno.h>
#include <string.h>

#include <hdfs.h>


struct xs_hdfsFS {
  hdfsFS obj;
  unsigned int refcnt;
};
typedef struct xs_hdfsFS xs_hdfsFS_t;

#define xs_hdfsFS_RefCntDec(fs) \
  STMT_START { \
    if ( --((fs)->refcnt) == 0 ) { \
      hdfsDisconnect(fs); \
      /* FIXME check for necessity of free() */\
    } \
  } STMT_END

#define xs_hdfsFS_refCntInc(fs) \
  STMT_START { \
    ++((fs)->refcnt); \
  } STMT_END

#define xs_hdfsFS_alloc(target, o) \
  STMT_START { \
    (target) = (xs_hdfsFS_t *) malloc(sizeof(xs_hdfsFS_t)); \
    (target)->obj = (o); \
    (target)->refcnt = 1; \
  } STMT_END

struct xs_hdfsFile {
  hdfsFile obj;
  xs_hdfsFS_t *parent;
  unsigned int refcnt;
};
typedef struct xs_hdfsFile xs_hdfsFile_t;

#define xs_hdfsFile_RefCntDec(retval, file) \
  STMT_START { \
    if ( --((file)->refcnt) == 0 ) { \
      retval = hdfsCloseFile((file)->parent->obj, (file)->obj); \
      xs_hdfsFS_RefCntDec((file)->parent); \
      /* FIXME check for necessity of free() */ \
    } \
  } STMT_END

#define xs_hdfsFile_RefCntDec_ignore(file) \
  STMT_START { \
    if ( --((file)->refcnt) == 0 ) { \
      hdfsCloseFile((file)->parent->obj, (file)->obj); \
      xs_hdfsFS_RefCntDec((file)->parent); \
      /* FIXME check for necessity of free() */ \
    } \
  } STMT_END


#define xs_hdfsFile_refCntInc(file) \
  STMT_START { \
    ++((file)->refcnt); \
  } STMT_END

#define xs_hdfsFile_alloc(target, prent, file) \
  STMT_START { \
    target = (xs_hdfsFile_t *) malloc(sizeof(xs_hdfsFile_t)); \
    (target)->obj = file; \
    xs_hdfsFS_refCntInc(prent); \
    (target)->parent = prent; \
    (target)->refcnt = 1; \
  } STMT_END

/*
 * xs_hdfsFS_t *
 * hdfsConnectPath(CLASS, uri)
 *     char *CLASS;
 *     char *uri;
 *   PREINIT:
 *     hdfsFS *fs;
 *   CODE:
 *     fs = hdfsConnectPath(uri);
 *     if (fs == NULL)
 *       croak("hdfsConnectPath failed");
 *     xs_hdfsFS_alloc(RETVAL, fs);
 *   OUTPUT: RETVAL
 */


static SV *
file_info_to_hashref(pTHX_ hdfsFileInfo *fi)
{
  HV *h = newHV();
  sv_2mortal((SV *)h);

  if (fi->mKind == kObjectKindFile)
    hv_stores(h, "kind", newSVpvs("file"));
  else if (fi->mKind == kObjectKindDirectory)
    hv_stores(h, "kind", newSVpvs("directory"));
  else
    croak("Unknown HDFS object type!");

  hv_stores(h, "name", newSVpvn(fi->mName, strlen(fi->mName)));
  hv_stores(h, "lastmod", newSVuv(fi->mLastMod));
  hv_stores(h, "size", newSViv(fi->mSize));
  hv_stores(h, "replication", newSViv(fi->mReplication));
  hv_stores(h, "blocksize", newSViv(fi->mBlockSize));
  hv_stores(h, "owner", newSVpvn(fi->mOwner, strlen(fi->mOwner)));
  hv_stores(h, "group", newSVpvn(fi->mGroup, strlen(fi->mGroup)));
  hv_stores(h, "permissions", newSViv(fi->mPermissions));
  hv_stores(h, "lastaccess", newSVuv(fi->mLastAccess));

  return (SV *)newRV((SV *)h);
}


MODULE = Hadoop::libhdfs PACKAGE = Hadoop::libhdfs::FS

PROTOTYPES: DISABLE

xs_hdfsFS_t *
ConnectAsUser(CLASS, host, port, user)
    char *CLASS;
    char *host;
    tPort port;
    char *user;
  PREINIT:
    hdfsFS *fs;
  ALIAS:
    ConnectAsUser = 0
    ConnectAsUserNewInstance = 1
  CODE:
    if (ix == 0)
      fs = hdfsConnectAsUser(host, port, user);
    else
      fs = hdfsConnectAsUserNewInstance(host, port, user);
    if (fs == NULL)
      croak("HDFS connection failed");
    xs_hdfsFS_alloc(RETVAL, fs);
  OUTPUT: RETVAL

xs_hdfsFS_t *
Connect(CLASS, host, port)
    char *CLASS;
    char *host;
    tPort port;
  PREINIT:
    hdfsFS *fs;
  ALIAS:
    Connect = 0
    ConnectNewInstance = 1
  CODE:
    if (ix == 0)
      fs = hdfsConnect(host, port);
    else
      fs = hdfsConnectNewInstance(host, port);
    if (fs == NULL)
      croak("HDFS connection failed");
    xs_hdfsFS_alloc(RETVAL, fs);
  OUTPUT: RETVAL

void
DESTROY(self)
    xs_hdfsFS_t *self;
  PPCODE:
    xs_hdfsFS_RefCntDec(self);
    XSRETURN_EMPTY;


int
Exists(self, path)
    xs_hdfsFS_t *self;
    const char *path;
  CODE:
    /* FIXME figure out hdfsExists semantics. WTF? */
    RETVAL = !hdfsExists(self, path);
  OUTPUT: RETVAL


xs_hdfsFile_t *
OpenFile(self, path, flags, bufferSize, replication, blocksize)
    xs_hdfsFS_t *self;
    const char* path;
    int flags;
    int bufferSize;
    short replication;
    tSize blocksize;
  PREINIT:
    char *CLASS = "Hadoop::libhdfs::File";
    hdfsFile file;
  CODE:
    file = hdfsOpenFile(self->obj, path, flags, bufferSize, replication, blocksize);
    if (file == NULL)
      croak("hdfsOpenFile failed");
    xs_hdfsFile_alloc(RETVAL, self, file);
  OUTPUT: RETVAL


int
Copy(src_fs, src_path, dest_fs, dest_path)
    xs_hdfsFS_t *src_fs;
    char *src_path;
    xs_hdfsFS_t *dest_fs;
    char *dest_path;
  CODE:
    RETVAL = hdfsCopy(src_fs->obj, src_path, dest_fs->obj, dest_path);
    if (RETVAL == -1)
      croak("Failed to copy HDFS file");
  OUTPUT: RETVAL

int
Move(src_fs, src_path, dest_fs, dest_path)
    xs_hdfsFS_t *src_fs;
    char *src_path;
    xs_hdfsFS_t *dest_fs;
    char *dest_path;
  CODE:
    RETVAL = hdfsMove(src_fs->obj, src_path, dest_fs->obj, dest_path);
    if (RETVAL == -1)
      croak("Failed to move HDFS file");
  OUTPUT: RETVAL

int
Delete(self, path, recursive)
    xs_hdfsFS_t *self;
    char *path;
    int recursive = 0;
  CODE:
    RETVAL = hdfsDelete(self->obj, path, recursive);
    if (RETVAL == -1)
      croak("Failed to delete HDFS file");
  OUTPUT: RETVAL

int
Rename(src_fs, src_path, dest_path)
    xs_hdfsFS_t *src_fs;
    char *src_path;
    char *dest_path;
  CODE:
    RETVAL = hdfsRename(src_fs->obj, src_path, dest_path);
    if (RETVAL == -1)
      croak("Failed to rename HDFS file from '%s' to '%s'", src_path, dest_path);
  OUTPUT: RETVAL


void
GetWorkingDirectory(self)
    xs_hdfsFile_t *self;
  PREINIT:
    char buf[1024];
    char *tmp;
    dTARG;
  PPCODE:
    tmp = hdfsGetWorkingDirectory(self->obj, buf, 1024);
    if (tmp == NULL)
      croak("Failed to get HDFS working directory");
    XPUSHp(tmp, strlen(tmp));
    XSRETURN(1); 

int
SetWorkingDirectory(self, path)
    xs_hdfsFS_t *self;
    char *path;
  ALIAS:
    SetWorkingDirectory = 0
    hdfsCreateDirectory = 1
  CODE:
    if (ix == 0) {
      RETVAL = hdfsSetWorkingDirectory(self->obj, path);
      if (RETVAL == -1)
        croak("Failed to set HDFS working directory to '%s'", path);
    }
    else {
      RETVAL = hdfsCreateDirectory(self->obj, path);
      if (RETVAL == -1)
        croak("Failed to create HDFS directory at '%s'", path);
    }
  OUTPUT: RETVAL

int
SetReplication(self, path, nreplicas)
    xs_hdfsFS_t *self;
    char *path;
    int16_t nreplicas;
  CODE:
    RETVAL = hdfsSetReplication(self->obj, path, nreplicas);
    if (RETVAL == -1)
      croak("Failed to set number of replicas of HDFS file '%s' to %i", path, nreplicas);
  OUTPUT: RETVAL


tOffset
GetDefaultBlockSize(self)
    xs_hdfsFile_t *self;
  ALIAS:
    GetDefaultBlockSize = 0
    GetCapacity = 1
    GetUsed = 2
  CODE:
    if (ix == 0) {
      RETVAL = hdfsGetDefaultBlockSize(self->obj);
      if (RETVAL == -1)
        croak("Failed to get default HDFS block size");
    }
    else if(ix == 1) {
      RETVAL = hdfsGetCapacity(self->obj);
      if (RETVAL == -1)
        croak("Failed to get HDFS capacity");
    }
    else {
      RETVAL = hdfsGetUsed(self->obj);
      if (RETVAL == -1)
        croak("Failed to get HDFS used size");
    }
  OUTPUT: RETVAL


int
Chown(self, path, owner, group)
    xs_hdfsFS_t *self;
    char *path;
    char *owner;
    char *group;
  CODE:
    RETVAL = hdfsChown(self->obj, path, owner, group);
    if (RETVAL == -1)
      croak("Failed to chown HDFS file '%s' to owner '%s', group '%s'", path, owner, group);
  OUTPUT: RETVAL

int
Chmod(self, path, mode)
    xs_hdfsFS_t *self;
    char *path;
    char mode;
  CODE:
    RETVAL = hdfsChmod(self->obj, path, mode);
    if (RETVAL == -1)
      croak("Failed to chmod HDFS file '%s' to mode %i", path, (int)mode);
  OUTPUT: RETVAL

int
Utime(self, path, mtime, atime)
    xs_hdfsFS_t *self;
    char *path;
    tTime mtime;
    tTime atime;
  CODE:
    RETVAL = hdfsUtime(self->obj, path, mtime, atime);
    if (RETVAL == -1)
      croak("Failed to set HDFS file '%s's mtime to '%u' and atime to '%u'", path, mtime, atime);
  OUTPUT: RETVAL


SV *
GetPathInfo(self, path)
    xs_hdfsFS_t *self;
    char *path;
  PREINIT:
    hdfsFileInfo *fileInfo;
  CODE:
    fileInfo = hdfsGetPathInfo(self->obj, path);
    RETVAL = sv_2mortal(file_info_to_hashref(aTHX_ fileInfo));
    hdfsFreeFileInfo(fileInfo, 1);
  OUTPUT: RETVAL

SV *
ListDirectory(self, path)
    xs_hdfsFS_t *self;
    char *path;
  PREINIT:
    hdfsFileInfo *fileInfos;
    int numInfos = 0;
    int i;
    AV *av;
  CODE:
    fileInfos = hdfsListDirectory(self->obj, path, &numInfos);
    av = (AV *)sv_2mortal((SV *)newAV());
    av_extend(av, numInfos-1);
    for (i = 0; i < numInfos; ++i)
      av_store(av, i, file_info_to_hashref(aTHX_ &fileInfos[i]));
    RETVAL = sv_2mortal(newRV((SV *)av));
    hdfsFreeFileInfo(fileInfos, 1);
  OUTPUT: RETVAL


MODULE = Hadoop::libhdfs PACKAGE = Hadoop::libhdfs::File


void
DESTROY(self)
    xs_hdfsFile_t *self;
  PPCODE:
    xs_hdfsFile_RefCntDec_ignore(self);
    XSRETURN_EMPTY;


int
Seek(self, desiredPos)
    xs_hdfsFile_t *self;
    tOffset desiredPos;
  CODE:
    RETVAL = hdfsSeek(self->parent->obj, self->obj, desiredPos);
    if (RETVAL == -1)
      croak("Failed to seek on HDFS file");
  OUTPUT: RETVAL

tOffset
Tell(self)
    xs_hdfsFile_t *self;
  CODE:
    RETVAL = hdfsTell(self->parent->obj, self->obj);
    if (RETVAL == -1)
      croak("HDFS file tell() failed");
  OUTPUT: RETVAL


void
Read(self, bufferlength);
    xs_hdfsFile_t *self;
    tSize bufferlength;
  PREINIT:
    void *buffer;
    tSize actual_len;
  PPCODE:
    buffer = malloc(bufferlength);
    actual_len = hdfsRead(self->parent->obj, self->obj, buffer, bufferlength);
    if (actual_len == -1) {
      free(buffer);
      croak("Failed to read from HDFS file");
    }
    XPUSHs(sv_2mortal(newSVpvn(buffer, actual_len)));
    free(buffer);
    XSRETURN(1);

void
Pread(self, position, bufferlength);
    xs_hdfsFile_t *self;
    tOffset position;
    tSize bufferlength;
  PREINIT:
    void *buffer;
    tSize actual_len;
  PPCODE:
    buffer = malloc(bufferlength);
    actual_len = hdfsPread(self->parent->obj, self->obj, position, buffer, bufferlength);
    if (actual_len == -1) {
      free(buffer);
      croak("Failed to read from HDFS file");
    }
    XPUSHs(sv_2mortal(newSVpvn(buffer, actual_len)));
    free(buffer);
    XSRETURN(1);

int
Write(self, data)
    xs_hdfsFile_t *self;
    SV *data;
  PREINIT:
    char *buffer;
    STRLEN len;
  CODE:
    buffer = SvPV(data, len);
    RETVAL = hdfsWrite(self->parent->obj, self->obj, buffer, len);
    if (RETVAL == -1)
      croak("Failed to write to HDFS file");
  OUTPUT: RETVAL

int
Flush(self)
    xs_hdfsFile_t *self;
  CODE:
    RETVAL = hdfsFlush(self->parent->obj, self->obj);
    if (RETVAL == -1)
      croak("Failed to flush HDFS file");
  OUTPUT: RETVAL

int
HFlush(self)
    xs_hdfsFile_t *self;
  CODE:
    RETVAL = hdfsFlush(self->parent->obj, self->obj);
    if (RETVAL == -1)
      croak("Failed to flush HDFS file (%i): %s", errno, strerror(errno));
  OUTPUT: RETVAL

int
Available(self)
    xs_hdfsFile_t *self;
  CODE:
    RETVAL = hdfsAvailable(self->parent->obj, self->obj);
    if (RETVAL == -1)
      croak("Failed to check available bytes on HDFS file");
  OUTPUT: RETVAL

