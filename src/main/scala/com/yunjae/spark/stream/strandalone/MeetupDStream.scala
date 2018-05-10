package com.yunjae.spark.stream.strandalone

import net.liftweb.json.{parse, DefaultFormats}
import org.apache.spark.streaming.StreamingContext

case class Venue(venue_name: String, lat: Double, lon: Double)
case class Member(member_name: String, photo: Option[String])
case class Topic(topic_name: String, urlkey: String)
case class Event(event_name: String, event_url: String, time: Int)
case class Group(group_name: String, group_city: Option[String], group_count: Int)
case class RSVP(
  venue: Option[Venue],
  member: Member,
  group: Group,
  event: Event,
  response: String
)

object MeetupDStream {
  implicit val formats = DefaultFormats

  def apply(scc: StreamingContext) = {
    val meetupStream = scc.receiverStream(new MeetupReceiver)
    meetupStream
      .filter(line => line.startsWith("{"))
      .flatMap(line => try {
        Some(parse(line).extract[RSVP])
      } catch {
        case _: Throwable => None
      })
  }
}
