fs = require "fs"
mongoose = require "mongoose"

mongoose.connect "/tmp/mongodb-27017.sock/#{process.argv[3]}"