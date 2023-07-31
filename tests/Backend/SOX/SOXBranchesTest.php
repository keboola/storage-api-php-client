<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\SOX;

use Generator;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DevBranches;
use Keboola\Test\StorageApiTestCase;

class SOXBranchesTest extends StorageApiTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->cleanupTestBranches($this->getDeveloperStorageApiClient());
    }

    public function tokensProvider(): Generator
    {
        yield 'privileged token' => [
            'client' => $this->getDefaultBranchStorageApiClient(),
            'canManipulateBranch' => false,
        ];
        yield 'productionManager token' => [
            'client' => $this->getDefaultClient(),
            'canManipulateBranch' => false,
        ];
        yield 'developer token' => [
            'client' => $this->getDeveloperStorageApiClient(),
            'canManipulateBranch' => true,
        ];
        yield 'reviewer token' => [
            'client' => $this->getReviewerStorageApiClient(),
            'canManipulateBranch' => true,
        ];
        yield 'readOnly token' => [
            'client' => $this->getReadOnlyStorageApiClient(),
            'canManipulateBranch' => false,
        ];
    }

    /**
     * @dataProvider tokensProvider
     */
    public function testBranchesAccess(Client $client, bool $canManipulateBranch): void
    {
        if ($canManipulateBranch) {
            $this->expectNotToPerformAssertions();
        }

        $branches = new DevBranches($client);
        // everyone can list branches
        $branches->listBranches();

        ['id' => $testBranchId] = (new DevBranches($this->getDeveloperStorageApiClient()))->createBranch($this->generateDescriptionForTestObject());

        // everyone can get branch detail
        $branches->getBranch($testBranchId);

        $this->asserBranchCall(
            $canManipulateBranch,
            fn() => $branches->createBranch($this->generateDescriptionForTestObject() . '_')
        );

        $this->asserBranchCall(
            $canManipulateBranch,
            fn() => $branches->updateBranch($testBranchId, '', 'description')
        );

        $this->asserBranchCall(
            $canManipulateBranch,
            fn() => $branches->deleteBranch($testBranchId)
        );
    }

    private function asserBranchCall(bool $canManipulateBranch, callable $apiCall): void
    {
        try {
            $apiCall();
            if (!$canManipulateBranch) {
                $this->fail('Token should not be able to create branch');
            }
        } catch (ClientException $e) {
            if ($canManipulateBranch) {
                $this->fail('Token should be able to create branch');
            } else {
                $this->assertSame(403, $e->getCode());
                $this->assertSame('You don\'t have access to the resource.', $e->getMessage());
            }
        }
    }

    public function testDeveloperReviewerCanManageEachOtherBranches(): void
    {
        $this->expectNotToPerformAssertions();
        $developerClient = $this->getDeveloperStorageApiClient();
        $reviewerClient = $this->getReviewerStorageApiClient();

        $developerBranches = new DevBranches($developerClient);
        $reviewerBranches = new DevBranches($reviewerClient);

        // developer created branch reviver can manage
        ['id' => $testBranchId] = $developerBranches->createBranch($this->generateDescriptionForTestObject());
        $reviewerBranches->updateBranch($testBranchId, '', 'description');
        $reviewerBranches->deleteBranch($testBranchId);

        // reviver created branch developer can manage
        ['id' => $testBranchId] = $reviewerBranches->createBranch($this->generateDescriptionForTestObject());
        $developerBranches->updateBranch($testBranchId, '', 'description');
        $developerBranches->deleteBranch($testBranchId);
    }
}
