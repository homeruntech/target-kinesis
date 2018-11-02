# target-kinesis

A [Singer](https://singer.io) target that propagate data to [Amazon Kinesis](https://aws.amazon.com/kinesis/) and [Amazon Kinesis Data Firehose](https://aws.amazon.com/kinesis/data-firehose/) streams.

## How to use it

`target-kinesis` works together with any other [Singer Tap] to move data from sources like [Braintree], [Freshdesk] and [Hubspot] to Kinesis or Kinesis Firehose stream.

### Install and Run

First, make sure Python 3 is installed on your system or follow these
installation instructions for [Mac] or
[Ubuntu].


It's recommended to use a virtualenv:

```bash
 python3 -m venv ~/.virtualenvs/target-kinesis
 source ~/.virtualenvs/target-kinesis/bin/activate
 pip install -U pip setuptools
 pip install -e '.[dev]'
```

`target-kinesis` can be run with any [Singer Tap], it simply propagate records encoded using the Singer format to a Kinesis or Kinesis Firehose stream.

### Optional Configuration

`target-kinesis` requires configuration file that contains parameters to connect to AWS environment.

Here is an example of required configuration:
```
{
  "stream_name": "YOUR_KINESIS_STREAM_NAME",
  "aws_access_key_id": "YOUR_AWS_ACCESS_KEY_ID",
  "aws_secret_access_key": "YOUR_AWS_SECRET_ACCESS_KEY",
  "region": "YOUR_AWS_REGION",
  "is_firehose": true
}
```

Also available inside [config.sample.json](config.sample.json)

To run `target-kinesis` with the configuration file, use this command:

```bash
› tap-foobar | target-csv --config my-config.json
```

---

[Singer Tap]: https://singer.io
[Mac]: http://docs.python-guide.org/en/latest/starting/install3/osx/
[Ubuntu]: https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-ubuntu-16-04
