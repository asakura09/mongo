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

