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
