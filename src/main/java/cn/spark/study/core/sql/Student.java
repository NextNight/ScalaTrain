package cn.spark.study.core.sql;

import java.io.Serializable;

/**
 * Author:rzx
 * Date:2017/6/29
 */
public class Student {
        private String name;
        private Integer age;
        public Student(){}
        public Student(String name, Integer age) {
            this.name = name;
            this.age = age;
        }
        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }

}
