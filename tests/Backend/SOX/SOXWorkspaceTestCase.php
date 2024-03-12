<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\SOX;

use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\StorageApi\Tokens;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\ClientProvider\ClientProvider;
use Keboola\Test\StorageApiTestCase;

class SOXWorkspaceTestCase extends StorageApiTestCase
{
    public const TEST_FILE_WORKSPACE = false;

    protected ClientProvider $clientProvider;

    /** @var BranchAwareClient|Client */
    protected $workspaceSapiClient;

    public function setUp(): void
    {
        parent::setUp();
        $this->clientProvider = new ClientProvider($this);
        $currentToken = $this->_client->verifyToken();
        self::assertArrayHasKey('owner', $currentToken);

        $this->dropOldTokens();

        // default token can't create branch as it's not admin :sigh:
        $branches = new DevBranches($this->getDeveloperStorageApiClient());
        $branchPrefix = $this->clientProvider->getDevBranchName();
        $this->deleteBranchesByPrefix($branches, $branchPrefix);
        $branches->createBranch($branchPrefix);

        $developerClient = $this->clientProvider->createClientForCurrentTest($this->getClientOptionsForToken(STORAGE_API_DEVELOPER_TOKEN), true);
        $testToken = $this->initTestToken(new Tokens($this->getDeveloperStorageApiClient()));

        // so we reuse the just created branch
        $this->workspaceSapiClient = $this->clientProvider->createClientForCurrentTest(
            $this->getClientOptionsForToken($testToken),
            true,
        );

        $this->deleteOldTestWorkspaces($developerClient);
        $this->initEmptyTestBucketsForParallelTests([self::STAGE_OUT, self::STAGE_IN], $developerClient);
    }

    protected function initTestWorkspace(
        Client $client,
        ?string $backend = null,
        array $options = [],
        bool $forceRecreate = false
    ): array {
        if ($backend) {
            $options['backend'] = $backend;
        }

        $oldWorkspaces = $this->listTestWorkspaces($client);
        $workspaces = new Workspaces($this->workspaceSapiClient);

        $oldWorkspace = $oldWorkspaces ? reset($oldWorkspaces) : null;
        if (!$oldWorkspace) {
            return $workspaces->createWorkspace($options, true);
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

        return $workspaces->createWorkspace($options, true);
    }

    protected function deleteOldTestWorkspaces(Client $client): void
    {
        $workspaces = new Workspaces($client);

        foreach ($this->listTestWorkspaces($client) as $workspace) {
            $workspaces->deleteWorkspace($workspace['id'], [], true);
        }
    }

    protected function initTestToken(Tokens $tokens): string
    {
        $description = $this->generateDescriptionForTestObject();

        $tokenOptions = (new TokenCreateOptions())
            ->setCanManageBuckets(true)
            ->setDescription($description);

        $tokenData = $tokens->createToken($tokenOptions);
        return $tokenData['token'];
    }

    protected function listTestWorkspaces(Client $client): array
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

    public function dropOldTokens(): void
    {
        $tokensPrivileged = new Tokens($this->getClientForToken(STORAGE_API_DEFAULT_BRANCH_TOKEN));
        $description = $this->generateDescriptionForTestObject();
        $oldTestTokens = array_filter(
            $tokensPrivileged->listTokens(),
            function (array $token) use ($description) {
                return $token['description'] === $description;
            },
        );

        foreach ($oldTestTokens as $oldTestToken) {
            $tokensPrivileged->dropToken($oldTestToken['id']);
        }
    }
}
