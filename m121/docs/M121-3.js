//Lab - $group and Accumulators

var pipeline = [
  {
    $match: {
      awards: {$regex: /Won...Oscar/}
    }
  },
  {
    $group: {
      _id: null,
      max_rating: {$max: "$imdb.rating"},
      min_rating: {$min: "$imdb.rating"},
      avg_rating: {$avg: "$imdb.rating"},
      stds_rating: {$stdDevSamp: "$imdb.rating"}
    }
  }    
]

//answer

db.movies.aggregate([
  {
    $match: {
      awards: /Won \d{1,2} Oscars?/
    }
  },
  {
    $group: {
      _id: null,
      highest_rating: { $max: "$imdb.rating" },
      lowest_rating: { $min: "$imdb.rating" },
      average_rating: { $avg: "$imdb.rating" },
      deviation: { $stdDevSamp: "$imdb.rating" }
    }
  }
])

//Lab - $unwind

var pipeline = [
  {
    $match: {
      languages: "English",
      "imdb.rating": {$gte: 0}
    }
  },
  {

    $unwind: "$cast"

  },
  {
    $group: {
      _id: "$cast",
      numFilms: {$sum: 1},
      average: {$avg: "$imdb.rating"}
    }
  },
  {
    $sort:{
  
          numFilms: -1
  
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
      languages: "English"
    }
  },
  {
    $project: { _id: 0, cast: 1, "imdb.rating": 1 }
  },
  {
    $unwind: "$cast"
  },
  {
    $group: {
      _id: "$cast",
      numFilms: { $sum: 1 },
      average: { $avg: "$imdb.rating" }
    }
  },
  {
    $project: {
      numFilms: 1,
      average: {
        $divide: [{ $trunc: { $multiply: ["$average", 10] } }, 10]
      }
    }
  },
  {
    $sort: { numFilms: -1 }
  },
  {
    $limit: 1
  }
])