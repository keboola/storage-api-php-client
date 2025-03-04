<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 16/09/14
 * Time: 01:50
 * To change this template use File | Settings | File Templates.
 */

namespace Keboola\StorageApi\Options\Components;

class ListComponentsOptions
{
    private $componentType;

    private $include = [];

    private $isDeleted;

    /**
     * @return mixed
     */
    public function getComponentType()
    {
        return $this->componentType;
    }

    /**
     * @param mixed $componentType
     * @return $this
     */
    public function setComponentType($componentType): static
    {
        $this->componentType = $componentType;
        return $this;
    }

    public function toParamsArray(): array
    {
        return [
            'componentType' => $this->getComponentType(),
            'include' => implode(',', $this->getInclude()),
            'isDeleted' => $this->getIsDeleted(),
        ];
    }

    /**
     * @param array $include
     * @return $this
     */
    public function setInclude($include = []): static
    {
        $this->include = (array) $include;
        return $this;
    }

    public function getInclude()
    {
        return $this->include;
    }

    /**
     * @return bool|null
     */
    public function getIsDeleted()
    {
        return $this->isDeleted;
    }

    /**
     * @param bool $isDeleted
     * @return $this
     */
    public function setIsDeleted($isDeleted): static
    {
        $this->isDeleted = (bool) $isDeleted;
        return $this;
    }
}
