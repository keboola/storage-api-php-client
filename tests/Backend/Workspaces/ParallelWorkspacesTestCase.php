<?php
namespace Keboola\Test\Backend\Workspaces;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\StorageApi\Tokens;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\StorageApiTestCase;

abstract class ParallelWorkspacesTestCase extends StorageApiTestCase
{
    /** @var string */
    protected $runId;

    /** @var Client */
    protected $workspaceSapiClient;

    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();

        $this->workspaceSapiClient = $this->getClient([
            'token' => $this->initTestToken($this->tokens),
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
            'jobPollRetryDelay' => function () {
                return 1;
            },
        ]);
    }

    /**
     * Creates a new workspace for current test. If workspace already exist, resets its password.
     *
     * @param null|string $backend
     * @return array workspace detail
     */
    protected function initTestWorkspace($backend = null, array $options = [], bool $forceRecreate = false, bool $async = true)
    {
        if ($backend) {
            $options['backend'] = $backend;
        }

        $oldWorkspaces = $this->listTestWorkspaces($this->_client);
        $workspaces = new Workspaces($this->workspaceSapiClient);

        $oldWorkspace = $oldWorkspaces ? reset($oldWorkspaces) : null;
        if (!$oldWorkspace) {
            return $workspaces->createWorkspace($options, $async);
        }
        $couldReuseExistingWorkspace = !array_key_exists('backend', $options)
            || $oldWorkspace['connection']['backend'] === $options['backend'];
        if (!$forceRecreate && $couldReuseExistingWorkspace) {
            $result = $workspaces->resetWorkspacePassword($oldWorkspace['id']);
            if ($this->getDefaultBackend($this->workspaceSapiClient) === self::BACKEND_BIGQUERY) {
                $oldWorkspace['connection']['credentials'] = $result['credentials'];
            } else {
                $oldWorkspace['connection']['password'] = $result['password'];
            }
            return $oldWorkspace;
        }

        $this->deleteOldTestWorkspaces();
        return $workspaces->createWorkspace($options, $async);
    }

    protected function deleteOldTestWorkspaces()
    {
        $workspaces = new Workspaces($this->_client);

        foreach ($this->listTestWorkspaces($this->_client) as $workspace) {
            $workspaces->deleteWorkspace($workspace['id'], [], true);
        }
    }

    /**
     * @return string
     */
    protected function initTestToken(Tokens $tokens)
    {
        $description = $this->generateDescriptionForTestObject();

        $oldTestTokens = array_filter(
            $tokens->listTokens(),
            function (array $token) use ($description) {
                return $token['description'] === $description;
            },
        );

        foreach ($oldTestTokens as $oldTestToken) {
            if ($oldTestToken['canManageBuckets'] !== true
                || !array_key_exists('token', $oldTestToken)) {
                // projects with hide-decrypted-token feature does not contain token in response, so it cannot be reused
                $tokens->dropToken($oldTestToken['id']);
            } else {
                return $oldTestToken['token'];
            }
        }

        $tokenOptions = (new TokenCreateOptions())
            ->setCanManageBuckets(true)
            ->setDescription($this->generateDescriptionForTestObject());

        $tokenData = $tokens->createToken($tokenOptions);
        return $tokenData['token'];
    }

    protected function listWorkspaceJobs($workspaceId)
    {
        return array_filter(
            $this->_client->listJobs(),
            function ($job) use ($workspaceId) {
                $workspaceIdParam = isset($job['operationParams']['workspaceId']) ? (int) $job['operationParams']['workspaceId'] : 0;
                return (int) $workspaceIdParam === $workspaceId;
            },
        );
    }

    protected function listTestWorkspaces(Client $client)
    {
        $description = $this->generateDescriptionForTestObject();
        $workspaces = new Workspaces($client);

        return array_filter(
            $workspaces->listWorkspaces(),
            function (array $workspace) use ($description) {
                return $workspace['creatorToken']['description'] === $description;
            },
        );
    }
}
