#!/usr/bin/env python3
"""Render standalone wire schemas from the Go reference's descriptor set.

Language/validation annotations are omitted; fields, numbers, enums, oneofs,
JSON names and services are preserved. The Go server does not run PGV validation.
Run only when deliberately updating the pinned reference API.
"""
from pathlib import Path
import json
from google.protobuf import descriptor_pb2 as d

root = Path(__file__).resolve().parents[1]
files = d.FileDescriptorSet.FromString((root / 'proto/reference.pb').read_bytes()).file
skip = ('google/protobuf/', 'udpa/annotations/', 'xds/annotations/', 'envoy/annotations/', 'validate/')
scalars = {1:'double',2:'float',3:'int64',4:'uint64',5:'int32',6:'fixed64',7:'fixed32',8:'bool',9:'string',12:'bytes',13:'uint32',15:'sfixed32',16:'sfixed64',17:'sint32',18:'sint64'}

def enum(e, depth):
    pad = '  '*depth
    lines = [pad+'enum '+e.name+' {']
    if e.options.allow_alias: lines.append(pad+'  option allow_alias = true;')
    lines.extend(pad+f'  {v.name} = {v.number};' for v in e.value)
    return lines+[pad+'}']

def message(m, depth):
    pad = '  '*depth
    lines = [pad+'message '+m.name+' {']
    maps = {n.name:n for n in m.nested_type if n.options.map_entry}
    for e in m.enum_type: lines.extend(enum(e,depth+1))
    for n in m.nested_type:
        if not n.options.map_entry: lines.extend(message(n,depth+1))
    def field(f, dep, in_oneof=False):
        typ = scalars.get(f.type, f.type_name)
        label = 'repeated ' if f.label == 3 else ('optional ' if f.proto3_optional else '')
        entry = maps.get(f.type_name.rsplit('.',1)[-1]) if f.type == 11 and f.label == 3 else None
        if entry:
            k,v = entry.field
            typ = f'map<{scalars.get(k.type,k.type_name)}, {scalars.get(v.type,v.type_name)}>'
            label = ''
        if in_oneof: label = ''
        return '  '*dep+f'{label}{typ} {f.name} = {f.number} [json_name = {json.dumps(f.json_name)}];'
    for f in m.field:
        if not f.HasField('oneof_index') or f.proto3_optional: lines.append(field(f,depth+1))
    for i,o in enumerate(m.oneof_decl):
        fields = [f for f in m.field if f.HasField('oneof_index') and f.oneof_index==i and not f.proto3_optional]
        if fields:
            lines.append(pad+'  oneof '+o.name+' {')
            lines.extend(field(f,depth+2,True) for f in fields)
            lines.append(pad+'  }')
    lines.append(pad+'}')
    return lines

for f in files:
    if f.name.startswith(skip): continue
    lines=['// Derived from the pinned Go reference; see proto/reference.pb and NOTICE.', 'syntax = "proto3";',f'package {f.package};']
    lines.extend(f'import "{dep}";' for dep in f.dependency if not dep.startswith(skip[1:]))
    for e in f.enum_type: lines.extend(enum(e,0))
    for m in f.message_type: lines.extend(message(m,0))
    for s in f.service:
        lines.append('service '+s.name+' {')
        for method in s.method:
            inp=('stream ' if method.client_streaming else '')+method.input_type
            out=('stream ' if method.server_streaming else '')+method.output_type
            lines.append(f'  rpc {method.name} ({inp}) returns ({out});')
        lines.append('}')
    path=root/'proto'/f.name
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text('\n'.join(lines)+'\n')
