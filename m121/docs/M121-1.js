//connection
mongo "mongodb://cluster0-shard-00-00-jxeqq.mongodb.net:27017,cluster0-shard-00-01-jxeqq.mongodb.net:27017,cluster0-shard-00-02-jxeqq.mongodb.net:27017/aggregations?replicaSet=Cluster0-shard-0" --authenticationDatabase admin --ssl -u m121 -p aggregations --norc



// LAB 1 ATTEMPTS


var pipeline = [
    {
    $match:{
        "imdb.rating": {$gte: 7}, 
        genres: {$nin: ["Crime", "Horror"]}, 
        rated: {$in: ["PG", "G"]}, 
        {$and: [{languages: "Japanese"}, {languages: "English"}]}
        }
    }
]

var pipeline = [
    {
    $match:{
        "imdb.rating": {$gte: 7}, 
        genres:{$nin: ["Crime", "Horror"]}, 
        rated:{$in: ["PG", "G"]},
        languages: {$all: ["Japanese", "English"]}
    }
}
]

//Lab - Changing Document Shape with $project

var pipeline = [
    {
    $match:{
        "imdb.rating": {$gte: 7}, 
        genres:{$nin: ["Crime", "Horror"]}, 
        rated:{$in: ["PG", "G"]},
        languages: {$all: ["Japanese", "English"]}
        }
    },
    {
    $project:{
        _id: 0,
        title: 1,
        rated: 1
        }
    }
]

//Lab - Computing Fields

var pipeline = [
    {
    $project:{
        title: {$split: ["$title", " "]}    
        }
    },  
    {
    $project:{
            titlesize: {$size: "$title"}
        }
    },
    {
    $match:{

        titlesize:{$eq: 1}
        }  
    }   
]

//respuesta:

db.movies.aggregate([
    {
      $match: {
        title: {
          $type: "string"
        }
      }
    },
    {
      $project: {
        title: { $split: ["$title", " "] },
        _id: 0
      }
    },
    {
      $match: {
        title: { $size: 1 }
      }
    }
  ]).itcount()

  //Optional Lab - Expressions with $project

  //$map example

  writers: {
    $map: {
      input: "$writers",
      as: "writer",
      in: {
        $arrayElemAt: [
          {
            $split: [ "$$writer", " (" ]
          },
          0
        ]
      }
    }
  }

//answer attempt

db.movies.aggregate([
  {
    $match: {
      writers: { $elemMatch: { $exists: true }},
      cast: { $elemMatch: { $exists: true }},
      directors: { $elemMatch: { $exists: true }}
    }
  },
  {
    $project: {
      writers: {
        $map: {
          input: "$writers",
          as: "writer",
          in: {
            $arrayElemAt: [
              {
                $split: [ "$$writer", " (" ]
              },
              0
            ]
          }
        }
      },
      cast: 1,
      directors: 1,
      _id: 0
    }
  },
  {
    $project: {
      laborsOfLove: {$size: {$setIntersection: ["$writers", "$cast", "$directors"]}}
    }
  },
  {
    $match: {
      laborsOfLove: { $gt: 0}
    }
  }
]).itcount()

//answer
db.movies.aggregate([
  {
    $match: {
      cast: { $elemMatch: { $exists: true } },
      directors: { $elemMatch: { $exists: true } },
      writers: { $elemMatch: { $exists: true } }
    }
  },
  {
    $project: {
      _id: 0,
      cast: 1,
      directors: 1,
      writers: {
        $map: {
          input: "$writers",
          as: "writer",
          in: {
            $arrayElemAt: [
              {
                $split: ["$$writer", " ("]
              },
              0
            ]
          }
        }
      }
    }
  },
  {
    $project: {
      labor_of_love: {
        $gt: [
          { $size: { $setIntersection: ["$cast", "$directors", "$writers"] } },
          0
        ]
      }
    }
  },
  {
    $match: { labor_of_love: true }
  },
  {
    $count: "labors of love"
  }
])

//Filter out documents that are not an array 
//or have an empty array for the fields we are interested in:

{
  $match: {
    cast: { $elemMatch: { $exists: true } },
    directors: { $elemMatch: { $exists: true } },
    writers: { $elemMatch: { $exists: true } }
  }
}