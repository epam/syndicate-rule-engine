# add

## Description

Creates Custodian Service Mail configuration

## Synopsis

```bash
c7n setting mail add
    --host <text>
    --password <text>
    --password_label <text>
    --port <integer>
    --username <text>
    [--emails_per_session <integer>]
    [--sender_name <text>]
    [--use_tls <boolean>]
```

## Options

`--host` (text) 

Host of a mail server.

`--password` (text) 

Password of mail account.

`--password_label` (text) 

Name of the parameter to store password under.

`--port` (integer) 

Port of a mail server.

`--username` (text) 

Username of mail account.

`--emails_per_session` (integer) [default: 1]

Amount of emails to send per session.

`--sender_name` (text) 

Name to specify as the sender of email(s). Defaults to '--username'.

`--use_tls` (boolean) 

Specify to whether utilize TLS.


[← mail](./index.md)