# Userfeeds Algorithms

(Apply for access to developer VPN and database credentials at developers@userfeeds.io)

1. Change `NEO4J_AUTH=myneologin/myneopassword` in `api-local.docker.yml`
2. Connect to VPN
3. Run `docker-compose -f api-local.docker.yml up --build`
4. The API will be available at http://localhost:8000/

Now you will be able to create your custom algorithms and add them to Userfeeds algortihm repository.

You can check status of the API by calling `curl http://localhost:8000/status` from command line.
