package com.example

case class Movie(_id: String, imdb_title_id: String, title: String, original_title: String, year: Option[Int], date_published: String, genre: String,
                 duration: Option[Int], country_id: Option[String], language: Option[String], director: Option[String], writer: Option[String],
                 production_company: Option[String], actors: Option[String], description: Option[String], avg_vote: Double, votes: Int,
                 budget: Option[String], usa_gross_income: Option[String], worlwide_gross_income: Option[String], metascore: Option[Double],
                 reviews_from_users: Option[Double], reviews_from_critics: Option[Double]) {
  def as[T](implicit f: Movie => T) = f(this)
}

object Movie {
  implicit def movieMapper = (movieEntity: Movie) =>
    FullMovie(
      movieEntity._id,
      movieEntity.imdb_title_id,
      movieEntity.title,
      movieEntity.original_title,
      movieEntity.year,
      movieEntity.date_published,
      movieEntity.genre,
      movieEntity.duration,
      movieEntity.country_id,
      country = None,
      movieEntity.language,
      movieEntity.director,
      movieEntity.writer,
      movieEntity.production_company,
      movieEntity.actors,
      movieEntity.description,
      movieEntity.avg_vote,
      movieEntity.votes,
      movieEntity.budget,
      movieEntity.usa_gross_income,
      movieEntity.worlwide_gross_income,
      movieEntity.metascore,
      movieEntity.reviews_from_users,
      movieEntity.reviews_from_critics
    )
}