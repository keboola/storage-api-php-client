<?php
namespace Keboola\StorageApi\Options\Components;

class ListComponentConfigurationsOptions
{
    private $componentId;

    private $isDeleted;

    public function toParamsArray()
    {
        return array(
            'isDeleted' => $this->getIsDeleted(),
        );
    }

    /**
     * @return mixed
     */
    public function getComponentId()
    {
        return $this->componentId;
    }

    /**
     * @param mixed $componentId
     * @return $this
     */
    public function setComponentId($componentId)
    {
        $this->componentId = $componentId;
        return $this;
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
    public function setIsDeleted($isDeleted)
    {
        $this->isDeleted = (bool) $isDeleted;
        return $this;
    }
}
