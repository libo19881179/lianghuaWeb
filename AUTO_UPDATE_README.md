# 自动更新服务使用说明

## 功能介绍

本服务用于每天凌晨02:00自动更新全市场A股股票数据，确保数据的及时性和完整性。

## 文件结构

- `auto_update_service.py` - 自动更新服务的核心脚本
- `start_auto_update.bat` - 启动脚本
- `auto_update.log` - 自动生成的日志文件

## 依赖项

- Python 3.7+
- schedule 库
- 项目其他依赖项（已在项目中配置）

## 安装步骤

1. **安装schedule库**
   ```bash
   pip install schedule
   ```

2. **配置服务**
   - 打开 `auto_update_service.py` 文件
   - 根据需要修改更新时间（默认为每天02:00）

3. **启动服务**
   - 双击运行 `start_auto_update.bat` 脚本
   - 或在命令行中执行：
     ```bash
     python auto_update_service.py
     ```

## 运行模式

### 手动运行
- 直接运行 `start_auto_update.bat` 脚本
- 服务会在控制台中运行，显示实时日志
- 按 Ctrl+C 停止服务

### 后台运行（推荐）
- 使用 Windows 任务计划程序将服务设置为后台运行
- 这样可以确保服务在系统重启后自动启动

## 日志管理

- 服务会生成 `auto_update.log` 日志文件
- 日志包含更新过程的详细信息
- 建议定期清理日志文件，避免占用过多磁盘空间

## 故障排查

1. **服务启动失败**
   - 检查Python环境是否正确配置
   - 检查依赖项是否安装完整
   - 查看 `auto_update.log` 日志文件

2. **更新失败**
   - 检查网络连接是否正常
   - 检查Baostock API是否可用
   - 查看日志文件中的错误信息

3. **服务停止运行**
   - 检查系统是否重启
   - 检查是否有未处理的异常
   - 考虑将服务设置为系统服务，确保自动重启

## 自定义配置

1. **修改更新时间**
   - 在 `auto_update_service.py` 文件中修改：
     ```python
     schedule.every().day.at("02:00").do(update_all_stocks)
     ```

2. **修改更新范围**
   - 可以在 `update_all_stocks` 函数中添加参数，控制更新的股票范围

3. **添加邮件通知**
   - 可以在更新完成后添加邮件通知功能，发送更新结果

## 注意事项

- 确保系统在更新时间（凌晨02:00）处于开机状态
- 确保网络连接稳定
- 定期检查日志文件，了解更新状态
- 考虑设置系统任务计划，确保服务自动启动

## 示例日志

```
2026-03-09 02:00:00 - auto_update_service - INFO - === 开始自动更新股票数据 ===
2026-03-09 02:00:01 - data_sources - INFO - 2026-03-09 是 交易日
2026-03-09 02:00:02 - data_sources - INFO -   从 Baostock 获取 A 股列表...
2026-03-09 02:00:10 - data_sources - INFO -   ✓ Baostock 获取成功：7132 只股票
2026-03-09 02:00:10 - data_sources - INFO -   ✓ 筛选后 A 股数量：5393 只
2026-03-09 02:00:10 - auto_update_service - INFO - 更新完成：
2026-03-09 02:00:10 - auto_update_service - INFO - 总股票数：5393
2026-03-09 02:00:10 - auto_update_service - INFO - 缓存完整：1200
2026-03-09 02:00:10 - auto_update_service - INFO - 新股票：0
2026-03-09 02:00:10 - auto_update_service - INFO - 已更新：4193
2026-03-09 02:00:10 - auto_update_service - INFO - 失败：0
2026-03-09 02:00:10 - auto_update_service - INFO - === 自动更新完成 ===
```
