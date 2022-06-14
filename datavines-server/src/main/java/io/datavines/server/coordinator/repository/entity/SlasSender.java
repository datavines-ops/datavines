package io.datavines.server.coordinator.repository.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.time.LocalDateTime;

@Data
@TableName("dv_slas_sender")
@EqualsAndHashCode
@ToString
public class SlasSender {
    private static final long serialVersionUID = -1L;

    @TableId(type = IdType.ASSIGN_ID)
    private Long id;

    @TableField(value = "work_space_id")
    private Long workSpaceId;

    @TableField(value = "type")
    private String type;

    @TableField(value = "name")
    private String name;

    @TableField(value = "config")
    private String config;

    @TableField(value = "create_by")
    private Long createBy;

    @TableField(value = "create_time")
    @JsonFormat(pattern = "uuuu-MM-dd HH:mm:ss", timezone = "GMT+8")
    private LocalDateTime createTime;

    @TableField(value = "update_by")
    private Long updateBy;

    @TableField(value = "update_time")
    @JsonFormat(pattern = "uuuu-MM-dd HH:mm:ss", timezone = "GMT+8")
    private LocalDateTime updateTime;
}