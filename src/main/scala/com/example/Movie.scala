package com.example

case class Movie(_id: String, imdb_title_id: String, title: String, original_title: String, year: Option[Int], date_published: String, genre: String,
                 duration: Option[Int], country_id: Option[String], language: Option[String], director: Option[String], writer: Option[String],
                 production_company: Option[String], actors: Option[String], description: Option[String], avg_vote: Double, votes: Int,
                 budget: Option[String], usa_gross_income: Option[String], worlwide_gross_income: Option[String], metascore: Option[Double],
                 reviews_from_users: Option[Double], reviews_from_critics: Option[Double])