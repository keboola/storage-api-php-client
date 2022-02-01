<?php

namespace Keboola\Test\Common;

use InvalidArgumentException;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ListComponentsOptions;
use Keboola\Test\StorageApiTestCase;

class BranchAwareClientTest extends StorageApiTestCase
{
    /**
     * @return void
     */
    public function setUp()
    {
        parent::setUp();

        $components = new Components($this->_client);
        foreach ($components->listComponents() as $component) {
            foreach ($component['configurations'] as $configuration) {
                $components->deleteConfiguration($component['id'], $configuration['id']);
            }
        }

        // erase all deleted configurations
        foreach ($components->listComponents((new ListComponentsOptions())->setIsDeleted(true)) as $component) {
            foreach ($component['configurations'] as $configuration) {
                $components->deleteConfiguration($component['id'], $configuration['id']);
            }
        }
    }

    /**
     * @dataProvider provideDefaultBranchUsage
     * @param string $defaultBranchUsage
     * @return void
     */
    public function testClientWithDefaultBranch($defaultBranchUsage)
    {
        switch ($defaultBranchUsage) {
            case 'id':
                $devBranch = new DevBranches($this->_client);
                $defaultBranches = array_filter(
                    $devBranch->listBranches(),
                    function (array $branch) {
                        return $branch['isDefault'] === true;
                    }
                );
                $defaultBranch = reset($defaultBranches);
                $branchClient = $this->getBranchAwareDefaultClient($defaultBranch['id']);
                break;
            case 'placeholder':
                $branchClient = $this->getBranchAwareDefaultClient('default');
                break;
            default:
                $this->fail('Unknown type');
        }

        $branchComponents = new Components($branchClient);
        $components = new Components($this->_client);

        // create new configurations in main branch
        $componentId = 'transformation';
        $configurationId = 'main-1';
        $configurationOptions = (new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setName('Main 1')
            ->setConfiguration(['test' => 'false']);

        $components->addConfiguration($configurationOptions);
        $configuration = $components->getConfiguration($componentId, $configurationId);

        $this->assertSame(
            $configuration,
            $branchComponents->getConfiguration($componentId, $configurationId)
        );

        $this->assertSame(
            $components->listComponents(),
            $branchComponents->listComponents()
        );
    }

    /**
     * @return array
     */
    public function provideDefaultBranchUsage()
    {
        return [
            'use default branch by id' => ['id', null],
            'use default branch by placeholder' => ['placeholder', 'default'],
        ];
    }

    /**
     * @dataProvider provideInvalidBranchId
     * @param scalar|null $branchId
     * @return void
     */
    public function testInvalidBranch($branchId)
    {
        $options = [
            'token' => STORAGE_API_TOKEN,
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
            'jobPollRetryDelay' => function () {
                return 1;
            },
        ];
        $options['userAgent'] = $this->buildUserAgentString(
            $options['token'],
            $options['url']
        );
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(sprintf('Branch "%s" is not valid.', $branchId));
        new BranchAwareClient($branchId, $options);
    }

    /**
     * @return array
     */
    public function provideInvalidBranchId()
    {
        return [
            'empty string' => [''],
            'empty number' => [0],
        ];
    }
}
