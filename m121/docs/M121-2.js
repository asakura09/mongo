//Lab: Using Cursor-like Stages

var pipeline = [
  {
    $match: {
      cast: { $elemMatch: { $exists: true } },
      countries: { $elemMatch: { $exists: true } }
    }
  },
  {
  $match:{
      countries: "USA",
      "tomatoes.viewer.rating": {$gte: 3}
         
      }
  },  
  {
  $project:{
          _id: 0,
          num_favs: {$size: { $setIntersection: ["$cast", [
            "Sandra Bullock",
            "Tom Hanks",
            "Julia Roberts",
            "Kevin Spacey",
            "George Clooney"]] }},
          "tomatoes.viewer.rating":1,
          title:1
      }
  },
  {
  $sort:{

        num_favs: -1,
        "tomatoes.viewer.rating": -1,
        title:-1

      }  
  },
  {
  $limit:25  
  }   
]

//answer

var favorites = [
  "Sandra Bullock",
  "Tom Hanks",
  "Julia Roberts",
  "Kevin Spacey",
  "George Clooney"]

db.movies.aggregate([
  {
    $match: {
      "tomatoes.viewer.rating": { $gte: 3 },
      countries: "USA",
      cast: {
        $in: favorites
      }
    }
  },
  {
    $project: {
      _id: 0,
      title: 1,
      "tomatoes.viewer.rating": 1,
      num_favs: {
        $size: {
          $setIntersection: [
            "$cast",
            favorites
          ]
        }
      }
    }
  },
  {
    $sort: { num_favs: -1, "tomatoes.viewer.rating": -1, title: -1 }
  },
  {
    $skip: 24
  },
  {
    $limit: 1
  }
])

//Lab - Bringing it all together

var x_max = 1521105
var x_min = 5
var min = 1
var max = 10

var pipeline = [
  {
    $match: {
      languages: "English",
      "imdb.rating": { $gte: 1},
      "imdb.votes": { $gte: 1},
      year: {$gte: 1990}
    }
  }, 
  {
  $project:{
          _id: 0,
          "imdb.rating": 1,
          title: 1,
          scaled_vote:{
            $add: [
              1,
              {
                $multiply: [
                  9,
                  {
                    $divide: [
                      { $subtract: ["$imdb.votes", x_min] },
                      { $subtract: [x_max, x_min] }
                    ]
                  }
                ]
              }
            ]
          }
      }
  },
  {
    $addFields:{
      normalized_rating:{$avg: ["$scaled_vote", "$imdb.rating"]}
    }
  },
  {
  $sort:{
        normalized_rating: 1
      }  
  },
  {
  $limit:1  
  }   
]

//answer

db.movies.aggregate([
  {
    $match: {
      year: { $gte: 1990 },
      languages: { $in: ["English"] },
      "imdb.votes": { $gte: 1 },
      "imdb.rating": { $gte: 1 }
    }
  },
  {
    $project: {
      _id: 0,
      title: 1,
      "imdb.rating": 1,
      "imdb.votes": 1,
      normalized_rating: {
        $avg: [
          "$imdb.rating",
          {
            $add: [
              1,
              {
                $multiply: [
                  9,
                  {
                    $divide: [
                      { $subtract: ["$imdb.votes", 5] },
                      { $subtract: [1521105, 5] }
                    ]
                  }
                ]
              }
            ]
          }
        ]
      }
    }
  },
  { $sort: { normalized_rating: 1 } },
  { $limit: 1 }
])
