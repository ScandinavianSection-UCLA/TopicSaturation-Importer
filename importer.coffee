# Arguments: coffee importer.coffee {text-file} {corpus} {sub-corpus}

fs = require "fs"
mongoose = require "mongoose"
colors = require "colors"
async = require "async"

[_, _, textFile, corpus, subCorpus] = process.argv
mongoose.connect "/tmp/mongodb-27017.sock/stm_#{corpus}"

Topic = mongoose.model "Topic", new mongoose.Schema
	id: type: Number, index: true
	name: String

Record = mongoose.model "SubCorpus_#{subCorpus}", new mongoose.Schema
	article_id: String
	topic: type: mongoose.Schema.ObjectId, ref: "Topic"
	proportion: Number

processLine = (line, callback) ->
	return callback() if line[0] is "#"
	line = line
		.split /\s+/
		.filter (x) -> x isnt ""
	return callback() if line is []
	[_, article_id, tuples...] = line
	article_id = article_id.split("/")[-1..][0].split(".")[0]
	console.log "- decomposing article:".yellow, article_id
	[0..(tuples.length / 2)]
		.map (i) -> i * 2
	async.each [0...(tuples.length / 2)].map((i) -> i * 2), (i, callback) ->
		Topic.findOneAndUpdate {id: id = Number tuples[i]}, {$setOnInsert: id: id, name: "Topic #{id}"},
			new: true, upsert: true, (err, topic) ->
				if err?
					console.error "- error:".redBG, err
					return callback err
				new Record article_id: article_id, topic: topic._id, proportion: Number tuples[i + 1]
					.save (err, record) ->
						if err?
							console.error "- error:".redBG, err
							return callback err
						console.log "- inserted:".cyan, "article_id: #{record.article_id}, topic_id: #{topic.id}, proportion: #{record.proportion}"
						callback()
	, callback

fstr = []
fin = fs.createReadStream textFile, encoding: "utf8"
fin.on "data", (chunk) ->
	fin.pause()
	chunk = chunk.split /[\r\n]+/
	if fstr.length > 0
		fstr[fstr.length - 1] += chunk[0]
		chunk = chunk[1..]
	fstr = fstr.concat chunk
	async.each fstr[...-1], processLine, ->
		fstr = fstr[-1..]
		console.log "- processed chunk".green
		fin.resume()

fin.on "end", ->
	processLine fstr[0], ->
		console.log "- done".green