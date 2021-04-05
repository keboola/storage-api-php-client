<?php
namespace Keboola\Test\Backend\Workspaces;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\StorageApiTestCase;

abstract class ParallelWorkspacesTestCase extends StorageApiTestCase
{
    /** @var string */
    protected $runId;

    /** @var Client */
    protected $workspaceSapiClient;

    public function setUp()
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();

        $this->workspaceSapiClient = $this->getClient([
            'token' => $this->initTestToken(),
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
            'jobPollRetryDelay' => function () {
                return 1;
            },
        ]);
    }

    /**
     * Drops all workspaces created by current test and creates a new one
     *
     * @param array $options
     * @return array workspace detail
     */
    protected function recreateTestWorkspace($options = [])
    {
        $this->deleteOldTestWorkspaces();
        $workspaces = new Workspaces($this->workspaceSapiClient);
        return $workspaces->createWorkspace($options);
    }

    /**
     * Creates a new workspace for current test. If workspace already exist, resets its password
     *
     * @return array workspace detail
     */
    protected function initTestWorkspace()
    {
        $oldWorkspaces = $this->listTestWorkspaces();

        $workspaces = new Workspaces($this->workspaceSapiClient);

        if ($oldWorkspaces) {
            $oldWorkspace = reset($oldWorkspaces);
            $result = $workspaces->resetWorkspacePassword($oldWorkspace['id']);
            $oldWorkspace['connection']['password'] = $result['password'];
            return $oldWorkspace;
        } else {
            return $workspaces->createWorkspace();
        }
    }

    private function deleteOldTestWorkspaces()
    {
        $workspaces = new Workspaces($this->_client);

        foreach ($this->listTestWorkspaces() as $workspace) {
            $workspaces->deleteWorkspace($workspace['id'], [
                'async' => true,
            ]);
        }
    }

    /**
     * @return string
     */
    private function initTestToken()
    {
        $description = $this->generateDescriptionForTestObject();

        $oldTestTokens = array_filter(
            $this->tokens->listTokens(),
            function (array $token) use ($description) {
                return $token['description'] === $description;
            }
        );

        foreach ($oldTestTokens as $oldTestToken) {
            if ($oldTestToken['canManageBuckets'] !== true) {
                $this->tokens->dropToken($oldTestToken['id']);
            } else {
                return $oldTestToken['token'];
            }
        }

        $tokenOptions = (new TokenCreateOptions())
            ->setCanManageBuckets(true)
            ->setDescription($this->generateDescriptionForTestObject())
        ;

        $tokenData = $this->tokens->createToken($tokenOptions);
        return $tokenData['token'];
    }

    protected function listWorkspaceJobs($workspaceId)
    {
        return array_filter(
            $this->_client->listJobs(),
            function ($job) use ($workspaceId) {
                $workspaceIdParam = isset($job['operationParams']['workspaceId']) ? (int) $job['operationParams']['workspaceId'] : 0;
                return (int) $workspaceIdParam === $workspaceId;
            }
        );
    }

    private function listTestWorkspaces()
    {
        $description = $this->generateDescriptionForTestObject();
        $workspaces = new Workspaces($this->_client);

        return array_filter(
            $workspaces->listWorkspaces(),
            function (array $workspace) use ($description) {
                return $workspace['creatorToken']['description'] === $description;
            }
        );
    }
}
