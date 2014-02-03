# Arguments: coffee importer.coffee {text-file} {corpus} {sub-corpus}

fs = require "fs"
mongoose = require "mongoose"

[_, _, textFile, corpus, subCorpus] = process.argv
mongoose.connect "/tmp/mongodb-27017.sock/#{corpus}"

Topic = mongoose.model "Topic", new mongoose.Schema
	id: type: Number, index: true
	name: String

Record = mongoose.model subCorpus, new mongoose.Schema
	article_id: String
	topic: type: mongoose.Schema.ObjectId, ref: "Topic"
	proportion: Number

fs.readFileSync textFile
	.split /[\r\n]+/
	.map (x) -> x.split /\s+/g
	.forEach ([_, article_id, tuples...]) ->
		article_id = article_id.split(".")[0]
		[0..(tuples.length / 2)]
			.map (x) -> x * 2
			.forEach (i) ->
				Topic.findOneAndUpdate {id: id = Number tuples[i]}, {$setOnInsert: id: id, name: "Topic #{id}"},
					new: true, upsert: true, (topic) ->
						new Record article_id: article_id, topic: topic._id, proportion: Number tuples[i + 1]
							.save (err, record) ->
								console.log "- inserted:", record