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

        $description = get_class($this) . '\\' . $this->getName();

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
            $this->_client->listTokens(),
            function (array $token) use ($testName) {
                return $token['description'] === $testName;
            }
        );

        foreach ($oldTestTokens as $oldTestToken) {
            if ($oldTestToken['canManageBuckets'] !== true) {
                $this->_client->dropToken($oldTestToken['id']);
            } else {
                return $oldTestToken['token'];
            }
        }

        $tokenOptions = (new TokenCreateOptions())
            ->setCanManageBuckets(true)
            ->setDescription($testName)
        ;

        $tokenId = $this->_client->createToken($tokenOptions);
        $tokenData = $this->_client->getToken($tokenId);
        return $tokenData['token'];
    }
}
