package com.jmx.demos

/**
  *  @Created with IntelliJ IDEA.
  *  @author : jmx
  *  @Date: 2020/11/19
  *  @Time: 15:57
  *  */
class Entry {

}

case class Movies(
                   movieId: String, // 电影的id
                   title: String, // 电影的标题
                   genres: String // 电影类别
                 )

case class Ratings(
                    userId: String, // 用户的id
                    movieId: String, // 电影的id
                    rating: String, // 用户评分
                    timestamp: String // 时间戳
                  )

// 需求1MySQL结果表
case class tenGreatestMoviesByAverageRating(
                                             movieId: String, // 电影的id
                                             title: String, // 电影的标题
                                             avgRating: String // 电影平均评分
                                           )

// 需求2MySQL结果表
case class topGenresByAverageRating(
                                     genres: String, //电影类别
                                     avgRating: String // 平均评分
                                   )

// 需求3MySQL结果表
case class tenMostRatedFilms(
                              movieId: String, // 电影的id
                              title: String, // 电影的标题
                              ratingCnt: String // 电影被评分的次数
                            )