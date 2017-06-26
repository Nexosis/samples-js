## Nexosis API Sample Application
### Ruby on Rails <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/6/62/Ruby_On_Rails_Logo.svg/45px-Ruby_On_Rails_Logo.svg.png"/>

This application is a simple sample, and therefore relatively straightforward. Of course, there are still things you may want to know:

* **Ruby version** - _2.4.1p111 (2017-03-22 revision 58053)_

* **Application dependencies** - 
	* Nexosis API Gem (nexosis_api)
	* NVD3 charting for results page
	* Bootstrap for basic styling
	* Httparty for making api requests in api_client
	* a number of gems have been included for local ruby support in Visual Studio Code in which this was written

* **Configuration** - _Your config/secrets.yml file should contain an entry for 'api-key', or you should modify the use of the api key in the code. It's in there a lot, so I suggest updating the config._

* **Database creation** - _This application does not use a database - but the default rails setup of sqlite has been retained._

* **Tests** - Nothing in this projects right now. There are a slew of tests of the client [in that project](https://github.com/Nexosis/nexosisclient-rb)


#### Deployment instructions
If you have ruby and rails locally, I recommend using an IDE you're comfortable with and running debug mode.  This app has not been written with public deployment in mind and would allow any user to submit jobs to the Nexosis API which cost money - and has not otherwise been tested for performance, security, or anything other than basic function. **It's a sample to learn from - I wouldn't deploy it at all.**

>If you don't have or want Ruby, checkout the [docker folder](https://github.com/Nexosis/samples/tree/master/ruby-sample/docker). You can build a container to run locally.

