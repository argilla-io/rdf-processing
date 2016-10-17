import org.apache.commons.httpclient.URI

val URIPattern = "<([^>]+)>".r

val tokenPattern = "((?:[^\"\\\\]*(?:\\\\.[^\"\\\\]*)*(?:@\\w+(?:-\\w+)?|\\^\\^<[^>]+>)?)|<[^>]+>|\\_\\:\\w+|\\.)".r

val s = "blasd balakjsdflkjsadfsadlfjlsd @lf"

URIPattern.findFirstMatchIn(s).isEmpty


("asdf", "bd") match  {
  case (a, b) => a
  case s => s
}

new URI("<http://datos.bne.es/def/C1005>",true).getName

tokenPattern.findAllMatchIn("\"Joaquín Sorolla y Bastida (Valencia, 27 de febrero de 1863  - Cercedilla, provincia de Madrid, 10 de agosto  de 1923) fue un pintor español. Artista prolífico, dejó más de 2200 obras catalogadas. Su obra madura ha sido etiquetada como impresionista, postimpresionista y luminista.@es\"").toList