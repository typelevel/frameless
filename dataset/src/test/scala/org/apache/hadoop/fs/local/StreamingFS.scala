package org.apache.hadoop.fs.local

import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem
import org.apache.hadoop.fs.DelegateToFileSystem

class StreamingFS(uri: java.net.URI, conf: org.apache.hadoop.conf.Configuration)
    extends DelegateToFileSystem(
      uri,
      new BareLocalFileSystem(),
      conf,
      "file",
      false
    ) {}
