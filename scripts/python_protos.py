#!/usr/bin/env python3
"""Generate modern Python protobuf modules without grpcio-tools."""
from pathlib import Path
import sys
from google.protobuf import descriptor_pb2
descriptors = descriptor_pb2.FileDescriptorSet.FromString(Path(sys.argv[1]).read_bytes())
root=Path(sys.argv[2]); root.mkdir(parents=True,exist_ok=True)
for f in descriptors.file:
    if f.name.startswith('google/protobuf/'): continue
    name=f.name[:-6].replace('/','.')+'_pb2'
    path=root/(f.name[:-6]+'_pb2.py'); path.parent.mkdir(parents=True,exist_ok=True)
    lines=['# Generated; do not edit.', 'import importlib as _importlib', 'from google.protobuf import descriptor_pool as _pool', 'from google.protobuf.internal import builder as _builder']
    for dep in f.dependency: lines.append('_importlib.import_module('+repr(dep[:-6].replace('/','.')+'_pb2')+')')
    lines.extend(['DESCRIPTOR = _pool.Default().AddSerializedFile('+repr(f.SerializeToString())+')', '_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())', '_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, '+repr(name)+', globals())'])
    path.write_text('\n'.join(lines)+'\n')
