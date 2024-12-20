async function getPRDetails(): Promise<PRDetails> {
    core.info("Fetching PR details...");

    const eventPayload = JSON.parse(
            readFileSync(process.env.GITHUB_EVENT_PATH || "", "utf8")
            );

    let owner: string;
    let repo: string;
    let pullNumber: number;

    // Handle different event types
    if (eventPayload.issue) {
        // Comment event
        owner = eventPayload.repository.owner.login;
        repo = eventPayload.repository.name;
        pullNumber = eventPayload.issue.number;
    } else if (eventPayload.pull_request) {
        // Direct PR event
        owner = eventPayload.repository.owner.login;
        repo = eventPayload.repository.name;
        pullNumber = eventPayload.pull_request.number;
    } else {
        throw new Error("Could not determine PR details from event payload");
    }

    core.info(`Repository: ${owner}/${repo}`);
    core.info(`PR Number: ${pullNumber}`);

    try {
        const prResponse = await octokit.pulls.get({
                owner,
                repo,
                pull_number: pullNumber,
        });

        core.info(`PR details fetched for PR #${pullNumber}`);

        return {
            owner,
            repo,
            pull_number: pullNumber,
            title: prResponse.data.title ?? "",
            description: prResponse.data.body ?? "",
        };
    } catch (error) {
        core.error(`Failed to fetch PR details: ${error instanceof Error ? error.message : String(error)}`);
        throw error;
    }
}

interface PRDetails {
    owner: string;
    repo: string;
    pull_number: number;
    title: string;
    description: string;
}