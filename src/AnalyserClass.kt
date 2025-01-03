import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import java.io.File

suspend fun getPRDetails(): PRDetails = coroutineScope {
    core.info("Fetching PR details...")

    val eventPayload = readEventPayload()
    val (owner, repo, pullNumber) = extractPRDetails(eventPayload)

    core.info("Repository: $owner/$repo")
    core.info("PR Number: $pullNumber")

    fetchPRDetails(owner, repo, pullNumber)
}

private fun readEventPayload(): Map<String, Any> {
    val eventPath = System.getenv("GITHUB_EVENT_PATH") ?: ""
    return File(eventPath).readText().let { jsonString ->
        // Assuming a JSON parsing function is available
        parseJson(jsonString)
    }
}

private fun extractPRDetails(eventPayload: Map<String, Any>): Triple<String, String, Int> {
    val repository = eventPayload["repository"] as Map<String, Any>
    val owner = (repository["owner"] as Map<String, Any>)["login"] as String
    val repo = repository["name"] as String
    val pullNumber = when {
        eventPayload.containsKey("issue") -> (eventPayload["issue"] as Map<String, Any>)["number"] as Int
        eventPayload.containsKey("pull_request") -> (eventPayload["pull_request"] as Map<String, Any>)["number"] as Int
        else -> throw IllegalArgumentException("Could not determine PR details from event payload")
    }
    return Triple(owner, repo, pullNumber)
}

private suspend fun fetchPRDetails(owner: String, repo: String, pullNumber: Int): PRDetails {
    return try {
        val prResponse = octokit.pulls.get {
            this.owner = owner
            this.repo = repo
            this.pull_number = pullNumber
        }

        core.info("PR details fetched for PR #$pullNumber")

        PRDetails(
            owner = owner,
            repo = repo,
            pull_number = pullNumber,
            title = prResponse.data.title ?: "",
            description = prResponse.data.body ?: ""
        )
    } catch (error: Exception) {
        core.error("Failed to fetch PR details: ${error.message}")
        throw error
    }
}

data class PRDetails(
    val owner: String,
    val repo: String,
    val pull_number: Int,
    val title: String,
    val description: String
)