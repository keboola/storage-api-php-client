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

    private $include = array();

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
     */
    public function setComponentType($componentType)
    {
        $this->componentType = $componentType;
        return $this;
    }

    public function toParamsArray()
    {
        return array(
            'componentType' => $this->getComponentType(),
            'include' => implode(',', $this->getInclude()),
            'isDeleted' => $this->getIsDeleted(),
        );
    }

    /**
     * @param array $include
     * @return $this
     */
    public function setInclude($include = array())
    {
        $this->include = (array)$include;
        return $this;
    }

    public function getInclude()
    {
        return $this->include;
    }

    /**
     * @return mixed
     */
    public function getIsDeleted()
    {
        return $this->isDeleted;
    }

    /**
     * @param mixed $isDeleted
     * @return $this
     */
    public function setIsDeleted($isDeleted)
    {
        $this->isDeleted = $isDeleted;
        return $this;
    }
}
