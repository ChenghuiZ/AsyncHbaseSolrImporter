package com.chhz.persist.constant;

public enum EnumFieldUpdateType {
    REPLACE(1, "覆盖"),
    INCREASE(2, "增加");
    /**
     * ID
     */
    private int id;

    /**
     * 名称
     */
    private String name;

    EnumFieldUpdateType(int id, String name) {
        this.id = id;
        this.name = name;
    }

    /**
     * 获取指定的行为类型
     *
     * @param id
     * @return
     */
    public static EnumFieldUpdateType getOperationType(int id) {
        for (EnumFieldUpdateType operationType : EnumFieldUpdateType.values()) {
            if (operationType.id == id) {
                return operationType;
            }
        }
        return null;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
