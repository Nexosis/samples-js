Docker Image for Ruby Sample App
---
### Usage
If you have Docker locally you can build this image using the included compose file. If you don't have Docker - [go get it](https://docs.docker.com/engine/installation/). 

### Before you compose
- The secrets file also contains a reference to an environment variable named *NEXOSIS_API_KEY*. Set this environment variable on the host before you compose. The compose file will pick it up and pass it to your container. If you don't want to use an environment variable, just put your key directly in the *secrets.yml* file before you build.
- 
### Composing
Run the following in a command line from the docker files location...
``` bash
docker-compose -f ./ruby-compose.yml up -d
```
You can now browse to [http://localhost:3000/account]() and view the sample application. If there is a problem reported getting datasets it most likely means you've forgotten the API key entry in your environment.

#### Tearing Down
To stop the running server and remove the container go back to your shell and type
``` bash
docker-compose -f ./ruby-compose.yml down
```
to remove the built image *nexosis/ruby* include the rmi option
``` bash 
docker-compose -f ./ruby-compose.yml down --rmi all
```
> The included *secrets.yml* file has a secret key base for securing the web traffic. Obviously this isn't really secure, it's just for your private use on this sample app image. If you want to be sure you are not creating a server with this key, change it.

### Composing More Than Once
If you already have built the image nexosis/ruby, the secrets file will not be included again unless you force a rebuild by specifying --build after the *up* command to pick up changes in the secrets file.
``` bash
docker-compose -f ./ruby-compose.yml up -d --build
```