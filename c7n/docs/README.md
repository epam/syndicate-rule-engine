# Custodian as a service CLI (4.1.4) documentation



## c7n

### Description

Custodian as a service cli s a simple thin layer between you and Custodian as a service API. It provides command line interface to the API.


### Installation

**Standalone:**

```bash
pip install ./c7n
```

**m3-modular-admin:**

```bash
m3modular install --module_path ./c7n
```

Check whether CLI is installed:

```bash
$ c7n --version
c7n, version 4.1.4
```


### Synopsis

```bash
c7n <command> <subcommand> [parameters]
```

You can use `c7n <command> --help` to get information about specific command or group.


### Global Options

`--json` (flag)

Returns the output of API in json format directly to console - instead of table view which is by default.

`--verbose` (flag)

Writes verbose cli logs to `./c7ncli-logs` folder and prints trace id from AWS to console above the table.

`--help` (flag)

Outputs the information about command or group.

`--customer_id` (string)

Performs the action on behalf of the specified customer. Can be used only if you are a SYSTEM user

### Available command

- [application](./application/index.md)
- [cleanup](./cleanup.md)
- [configure](./configure.md)
- [customer](./customer/index.md)
- [health_check](./health_check.md)
- [job](./job/index.md)
- [lm](./lm/index.md)
- [login](./login.md)
- [parent](./parent/index.md)
- [policy](./policy/index.md)
- [report](./report/index.md)
- [results](./results/index.md)
- [role](./role/index.md)
- [rule](./rule/index.md)
- [ruleset](./ruleset/index.md)
- [rulesource](./rulesource/index.md)
- [setting](./setting/index.md)
- [show_config](./show_config.md)
- [siem](./siem/index.md)
- [tenant](./tenant/index.md)
- [trigger](./trigger/index.md)
- [user](./user/index.md)


_In case you have any questions, contact the support team_