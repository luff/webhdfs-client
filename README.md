# webhdfs-client

A linux command line webhdfs client.

## Installation
```
pip install webhdfs-client
```

or

```
pip install git+https://github.com/luff/webhdfs-client.git
```

## Configuration
put a .whdfsc.json in your home directory
```
{
  "insecure": false,
  "username": "your-webhdfs-user",
  "password": "your-webhdfs-user-pw",
  "rest_api": "https://your-webhdfs-gateway/webhdfs/v1"
}
```

## Usage
```
whdfsc --help
```

## License

See the [LICENSE](LICENSE) file for license rights and limitations (MIT).

