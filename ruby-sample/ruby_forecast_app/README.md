## Nexosis API Sample Application
### Ruby on Rails <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/6/62/Ruby_On_Rails_Logo.svg/45px-Ruby_On_Rails_Logo.svg.png"/>

This application is a simple sample, and therefore relatively straightforward. Of course, there are still things you may want to know:

* **Ruby version** - _2.4.1p111 (2017-03-22 revision 58053)_

* **System dependencies** - _none_

* **Configuration** - _Your config/secrets.yml file should contain an entry for 'api-key', or you should modify the use of the api key in the code. It's in there a lot, so I suggest updating the config._

* **Database creation** - _This application does not use a database._

* **How to run the test suite** - _pretty light on tests, but the api_client tests can be run by calling:_
 ```bash
 rake test TEST=test/apiclient/client_tests.rb
 ```


#### Deployment instructions
If you have ruby and rails locally, I recommend using an IDE you're comfortable with and running debug mode.  This app has not been written with public deployment in mind and would allow any user to submit jobs to the Nexosis API which cost money - and has not otherwise been tested for performance, security, or anything other than basic function. **It's a sample to learn from - I wouldn't deploy it at all.**

##### Comming Soon
* If you don't have a local environment, a docker compose will be coming soon.
* The api client is currently embedded into this application. We'll be packaging this as a gem. Once available this application code will be updated to use it.