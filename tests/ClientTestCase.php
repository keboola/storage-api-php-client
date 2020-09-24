<?php

namespace Keboola\Test;

use Keboola\StorageApi\Client;
use PHPUnit\Framework\TestCase;

class ClientTestCase extends TestCase
{
    /**
     * @return Client
     */
    protected function getClient(array $options)
    {
        $testSuiteName = '';
        if (SUITE_NAME) {
            $testSuiteName = sprintf('Suite: %s ', getenv('SUITE_NAME'));
        }

        $buildId = '';
        if (TRAVIS_BUILD_ID) {
            $buildId = sprintf('Build id: %s ', getenv('TRAVIS_BUILD_ID'));
        }

        $options['userAgent'] = sprintf('%s%sTest: %s', $buildId, $testSuiteName, $this->getTestName());
        return new Client($options);
    }

    /**
     * @return string
     */
    protected function getTestName()
    {
        return get_class($this) . '::' . $this->getName();
    }
}
