# Arguments: coffee importer.coffee {text-file} {corpus} {sub-corpus}

fs = require "fs"
mongoose = require "mongoose"
colors = require "colors"

[_, _, textFile, corpus, subCorpus] = process.argv
mongoose.connect "/tmp/mongodb-27017.sock/stm_#{corpus}"

Topic = mongoose.model "Topic", new mongoose.Schema
	id: type: Number, index: true
	name: String

Record = mongoose.model "SubCorpus_#{subCorpus}", new mongoose.Schema
	article_id: String
	topic: type: mongoose.Schema.ObjectId, ref: "Topic"
	proportion: Number

###
fstr = ""
fin = fs.createReadStream textFile, encoding: "utf8"
fin.on "data", (chunk) ->
	if (chunk = chunk.split /[\r\n]+/).length > 1
		(fstr + chunk[0])
			.split /\s+/g
			.filter (line) -> line isnt ""
###

fs.readFileSync textFile, encoding: "utf8"
	.split(/[\r\n]+/)[1..]
	.map (line, i) ->
		console.log "- decomposing line:".yellow, i
		line
			.split /\s+/g
			.filter (line) -> line isnt ""
	.filter (line) -> line isnt []
	.forEach ([_, article_id, tuples...]) ->
		console.log "- decomposing article:".yellow, article_id
		article_id = article_id.split(".")[0]
		[0..(tuples.length / 2)]
			.map (i) -> i * 2
			.forEach (i) ->
				Topic.findOneAndUpdate {id: id = Number tuples[i]}, {$setOnInsert: id: id, name: "Topic #{id}"},
					new: true, upsert: true, (err, topic) ->
						return console.error "- error:".redBG, err if err?
						new Record article_id: article_id, topic: topic._id, proportion: Number tuples[i + 1]
							.save (err, record) ->
								return console.error "- error:".redBG, err if err?
								console.log "- inserted:".cyan, record