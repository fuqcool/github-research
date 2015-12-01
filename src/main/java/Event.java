import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Event {
    private static final SimpleDateFormat dateFormat;
    private static final Map<String, Integer> scoreMap;

    static {
        dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

        scoreMap = new HashMap<String, Integer>();
        scoreMap.put("PushEvent", 50);
        scoreMap.put("CreateEvent", 50);
        scoreMap.put("WatchEvent", 10);
        scoreMap.put("ReleaseEvent", 50);
        scoreMap.put("PullRequestEvent", 50);
        scoreMap.put("IssuesEvent", 50);
        scoreMap.put("GollumEvent", 30);
        scoreMap.put("PublicEvent", 100);
        scoreMap.put("IssueCommentEvent", 10);
        scoreMap.put("CommitCommentEvent", 10);
        scoreMap.put("PullRequestReviewCommentEvent", 10);
        scoreMap.put("ForkEvent", 50);
    }

    private String type;
    private Date createdAt;
    private String repo;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Date getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Date createdAt) {
        this.createdAt = createdAt;
    }

    public String getRepo() {
        return repo;
    }

    public void setRepo(String repo) {
        this.repo = repo;
    }

    public int getScore() {
        Integer score = scoreMap.get(type);

        if (score == null) {
            return 0;
        }
        return score;
    }

    public static Event parseEvent(String s) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(s);

            Event evt = new Event();

            evt.type = root.path("type").getTextValue();
            evt.repo = root.path("repo").path("name").getTextValue();
            evt.createdAt = dateFormat.parse(root.path("created_at").getTextValue());

            return evt;
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return null;
        }
    }

}
