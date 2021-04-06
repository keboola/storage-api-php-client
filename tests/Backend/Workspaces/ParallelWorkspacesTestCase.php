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

        $description = $this->generateDescriptionForTestObject();

        $this->deleteOldTestWorkspaces($description);

        $this->workspaceSapiClient = $this->getClient([
            'token' => $this->initTestToken($description),
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
            'jobPollRetryDelay' => function () {
                return 1;
            },
        ]);
    }

    private function deleteOldTestWorkspaces($testName)
    {
        $workspaces = new Workspaces($this->_client);

        $oldTestWorkspaces = array_filter(
            $workspaces->listWorkspaces(),
            function (array $workspace) use ($testName) {
                return $workspace['creatorToken']['description'] === $testName;
            }
        );

        foreach ($oldTestWorkspaces as $workspace) {
            $workspaces->deleteWorkspace($workspace['id'], [
                'async' => true,
            ]);
        }
    }

    /**
     * @param string $testName
     * @return string
     */
    private function initTestToken($testName)
    {
        $oldTestTokens = array_filter(
            $this->tokens->listTokens(),
            function (array $token) use ($testName) {
                return $token['description'] === $testName;
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
            ->setDescription($testName)
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
}
