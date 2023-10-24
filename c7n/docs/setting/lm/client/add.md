# add

## Description

Adds License Manager provided client-key data

## Synopsis

```bash
c7n setting lm client add
    --algorithm <text>
    --key_id <text>
    --private_key <text>
    [--b64encoded <boolean>]
    [--format <PEM>]
```

## Options

`--algorithm` (text) 

Algorithm granted by the License Manager.

`--key_id` (text) 

Key-id granted by the License Manager.

`--private_key` (text) 

Private-key granted by the License Manager.

`--b64encoded` (boolean) 

Specify whether the private is b64encoded.

`--format` (PEM) [default: PEM]

Format of the private-key.


[← client](./index.md)