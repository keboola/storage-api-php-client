<?php

namespace Keboola\Test\Utils;

use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;

trait ComponentsConfigurationUtils
{
    /**
     * @param string|int $configurationId
     */
    public function createConfiguration(Components $components, string $componentId, $configurationId, string $name): Configuration
    {
        $configurationOptions = (new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setName($name);

        $components->addConfiguration($configurationOptions);
        return $configurationOptions;
    }
}
