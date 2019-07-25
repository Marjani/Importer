using System.Xml.Serialization;

[XmlRoot("rased_news")]
public class RasadNews
{

    [XmlElement("id")]
    public long Id { get; set; }

    [XmlElement("pageurl")]
    public string PageUrl { get; set; }

    [XmlElement("categorytype")]
    public string CategoryType { get; set; }

    [XmlElement("sourceid")]
    public string SourceId { get; set; }

    [XmlElement("title")]
    public string Title { get; set; }

    [XmlElement("description")]
    public string Description { get; set; }

    [XmlElement("content")]
    public string Content { get; set; }

    [XmlElement("author")]
    public string Author { get; set; }

    [XmlElement("pubtime")]
    public string PubTime { get; set; }

    [XmlElement("fetchtime")]
    public string FetchTime { get; set; }

    [XmlElement("mesbahdocid")]
    public string mesbahdocid { get; set; }

}