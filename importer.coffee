# Arguments: coffee importer.coffee {text-file} {corpus} {sub-corpus}

fs = require "fs"
mongoose = require "mongoose"
colors = require "colors"
async = require "async"

[_, _, textFile, corpus, subCorpus] = process.argv
mongoose.connect "/tmp/mongodb-27017.sock/stm_#{corpus}"

Topic = mongoose.model "Topic", new mongoose.Schema
	id: type: Number
	name: String

Record = mongoose.model "SubCorpus_#{subCorpus}", new mongoose.Schema
	article_id: String
	topic: type: mongoose.Schema.ObjectId, ref: "Topic"
	proportion: Number

topics = []

getOrInsertTopic = (id, callback) ->
	return callback null, topics[id] if topics[id]?
	Topic.findOneAndUpdate {id: id}, {$setOnInsert: id: id, name: "Topic #{id}"},
		new: true, upsert: true, callback

processLine = (line, callback) ->
	return callback null, 0 if line[0] is "#"
	line = line
		.split /\s+/
		.filter (x) -> x isnt ""
	return callback null, 0 if line is []
	[_, article_id, tuples...] = line
	try
		article_id = article_id.split("/")[-1..][0]
	catch ex
		console.error "Error: #{ex} in [#{line}]"
		process.exit 1
	# console.log "- decomposing article:".yellow, article_id
	async.map [0...(tuples.length / 2)].map((i) -> i * 2),
		(i, callback) ->
			getOrInsertTopic Number(tuples[i]), (err, topic) ->
				if err?
					console.error "- error:".redBG, err
					return callback err
				callback null, article_id: article_id, topic: topic._id, proportion: Number tuples[i + 1]
		(err, docs) ->
			Record.create docs, ->
				callback err, docs.length

fstr = []
fin = fs.createReadStream textFile, encoding: "utf8"
fin.on "data", (chunk) ->
	chunk = chunk.split /[\r\n]+/
	if fstr.length > 0
		fstr[fstr.length - 1] += chunk[0]
		chunk = chunk[1..]
	fstr = fstr.concat chunk
	if fstr.length > 100
		fin.pause()
		async.map fstr[...-1], processLine, (err, counts) ->
			count = counts.reduce (s, x) -> s + x
			return console.error "- error processing #{count} tuples".redBG, err if err?
			console.log "- processed #{count} tuples".green
			fin.resume()
		fstr = fstr[-1..]

fin.on "end", ->
	async.map fstr, processLine, (err, counts) ->
		count = counts.reduce (s, x) -> s + x
		if err?
			console.error "- error processing #{count} tuples".redBG, err
			return process.exit 1
		console.log "- processed #{count} tuples".green
		console.log "- done".green
		process.exit()
