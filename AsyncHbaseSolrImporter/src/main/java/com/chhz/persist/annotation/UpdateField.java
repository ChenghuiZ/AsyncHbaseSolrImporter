package com.chhz.persist.annotation;

import com.chhz.persist.constant.EnumFieldUpdateType;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 更新字段注解
 *
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface UpdateField {

    public EnumFieldUpdateType type() default EnumFieldUpdateType.REPLACE;

}
